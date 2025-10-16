package kz.qazmarka.h2k.kafka.producer.batch;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class BatchSenderTest {

    private static final ScheduledExecutorService EXEC =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "BatchSenderTest");
                t.setDaemon(true);
                return t;
            });

    @AfterAll
    static void shutdown() {
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
    @DisplayName("flush() выбрасывает TimeoutException при истечении дедлайна")
    void flushTimeout() throws Exception {
        try (BatchSender sender = new BatchSender(2, 50)) {
            CompletableFuture<RecordMetadata> pending = never();
            sender.add(pending);
            assertThrows(TimeoutException.class, sender::flush);
            pending.complete(null);
            sender.flush();
        }
    }

    @Test
    @DisplayName("close() ожидает оставшиеся futures")
    void closeFlushesPending() throws Exception {
        try (BatchSender sender = new BatchSender(10, 200)) {
            sender.add(ok());
        }
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
}
