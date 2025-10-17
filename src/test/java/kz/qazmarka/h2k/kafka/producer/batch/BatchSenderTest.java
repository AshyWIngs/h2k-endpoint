package kz.qazmarka.h2k.kafka.producer.batch;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class BatchSenderTest {

    private static final ScheduledExecutorService EXEC =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "BatchSenderTest");
                t.setDaemon(true);
                return t;
            });

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(BatchSenderTest::shutdownExecutor));
    }

    @AfterAll
    static void shutdownExecutor() {
        EXEC.shutdownNow();
    }

    @Test
    @DisplayName("add() автоматически вызывает flush по достижении порога")
    void addFlushesWhenThresholdReached() throws Exception {
        try (BatchSender sender = new BatchSender(2, 200)) {
            sender.add(ok());
            assertEquals(1, sender.pendingCount());

            sender.add(ok());
            assertEquals(0, sender.pendingCount(), "После второго add() буфер должен сброситься автоматически");
        }
    }

    @Test
    @DisplayName("addAll() выполняет промежуточные flush при большом наборе")
    void addAllFlushesInBatches() throws Exception {
        try (BatchSender sender = new BatchSender(3, 500)) {
            sender.addAll(Arrays.asList(ok(), ok(), ok(), ok(), ok()));
            // после addAll должно остаться 2 элемента (5 % 3)
            assertEquals(2, sender.pendingCount());
            sender.flush();
            assertEquals(0, sender.pendingCount());
        }
    }

    @Test
    @DisplayName("addAll() игнорирует null и пустые коллекции без побочных эффектов")
    void addAllIgnoresNullAndEmpty() throws Exception {
        try (BatchSender sender = new BatchSender(4, 200)) {
            sender.addAll(null);
            assertEquals(0, sender.pendingCount(), "null не должен менять состояние");

            sender.addAll(Collections.emptyList());
            assertEquals(0, sender.pendingCount(), "Пустая коллекция не должна добавлять записи");
        }
    }

    @Test
    @DisplayName("flush() ждёт завершения futures в пределах таймаута")
    void flushWaitsForFutures() throws Exception {
        try (BatchSender sender = new BatchSender(2, 500)) {
            sender.add(ok());
            sender.add(slow(100));
            sender.flush();
            assertEquals(0, sender.pendingCount());
        }
    }

    @Test
    @DisplayName("flush() спокойно обрабатывает пустой буфер")
    void flushHandlesEmptyBuffer() throws Exception {
        try (BatchSender sender = new BatchSender(3, 300)) {
            sender.flush();
            assertFalse(sender.hasPending(), "После flush() без данных буфер остаётся пустым");
        }
    }

    @Test
    @DisplayName("flush() выбрасывает TimeoutException при истечении дедлайна")
    void flushTimeout() throws Exception {
        try (BatchSender sender = new BatchSender(2, 50)) {
            CompletableFuture<RecordMetadata> pending = never();
            sender.add(pending);
            TimeoutException timeout = assertThrows(TimeoutException.class, sender::flush);
            assertNull(timeout.getMessage(), "TimeoutException по умолчанию не содержит сообщения");
            pending.complete(null);
            sender.flush();
        }
    }

    @Test
    @DisplayName("flush() пробрасывает ExecutionException, если отправка завершилась неудачно")
    void flushPropagatesExecutionException() throws Exception {
        try (BatchSender sender = new BatchSender(2, 200)) {
            sender.add(failed(new IllegalStateException("boom")));
            ExecutionException thrown = assertThrows(ExecutionException.class, sender::flush);
            assertEquals("boom", thrown.getCause().getMessage());
        } catch (ExecutionException expected) {
            assertEquals("boom", expected.getCause().getMessage(), "close() повторно пробрасывает исходную причину");
        }
    }

    @Test
    @DisplayName("add() игнорирует null и не меняет счётчики")
    void addIgnoresNull() throws Exception {
        try (BatchSender sender = new BatchSender(2, 200)) {
            sender.add(null);
            assertEquals(0, sender.pendingCount(), "После add(null) буфер должен оставаться пустым");
            assertFalse(sender.hasPending());
        }
    }

    @Test
    @DisplayName("close() с незавершённым future ожидает его")
    void closeWaitsForPending() throws Exception {
        CompletableFuture<RecordMetadata> slowFuture = slow(50);
        try (BatchSender sender = new BatchSender(10, 200)) {
            sender.add(slowFuture);
        }
        assertTrue(slowFuture.isDone(), "close() должен дождаться завершения всех futures");
    }

    @Test
    @DisplayName("close() ожидает оставшиеся futures")
    void closeFlushesPending() throws Exception {
        CompletableFuture<RecordMetadata> immediate = ok();
        assertTrue(immediate.isDone(), "Контроль: ok() возвращает завершённое future");
        try (BatchSender sender = new BatchSender(10, 200)) {
            sender.add(immediate);
            assertTrue(sender.hasPending(), "Перед закрытием ожидается хотя бы одна запись");
        }
    }

    @Test
    @DisplayName("Геттеры batchSize()/timeoutMs() возвращают параметры конструктора")
    void gettersExposeConfiguration() throws Exception {
        try (BatchSender sender = new BatchSender(5, 700)) {
            assertEquals(5, sender.batchSize());
            assertEquals(700, sender.timeoutMs());
        }
    }

    @Test
    @DisplayName("Конструктор валидирует batchSize")
    void constructorRejectsInvalidBatchSize() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new BatchSender(0, 100));
        assertEquals("batchSize должен быть > 0", ex.getMessage());
    }

    @Test
    @DisplayName("Конструктор валидирует timeoutMs")
    void constructorRejectsInvalidTimeout() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new BatchSender(1, 0));
        assertEquals("timeoutMs должен быть > 0", ex.getMessage());
    }

    private static CompletableFuture<RecordMetadata> ok() {
        return CompletableFuture.completedFuture(null);
    }

    private static CompletableFuture<RecordMetadata> slow(long delayMs) {
        CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
        EXEC.schedule(() -> cf.complete(null), delayMs, TimeUnit.MILLISECONDS);
        return cf;
    }

    private static CompletableFuture<RecordMetadata> never() {
        return new CompletableFuture<>();
    }

    private static CompletableFuture<RecordMetadata> failed(Throwable throwable) {
        CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
        cf.completeExceptionally(throwable);
        return cf;
    }
}
