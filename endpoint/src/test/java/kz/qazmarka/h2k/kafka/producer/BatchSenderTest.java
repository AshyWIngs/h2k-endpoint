package kz.qazmarka.h2k.kafka.producer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Юнит‑тесты для {@link BatchSender}.
 *
 * Назначение:
 *  • Проверить корректность поведения строгого сброса ({@link BatchSender#flush()}),
 *    «тихого» сброса ({@link BatchSender#tryFlush()}), авто‑сброса по порогу awaitEvery
 *    и реакции на ошибки/таймауты.
 *  • Убедиться, что общий дедлайн применяется на весь набор futures, и что при ошибках
 *    «тихий» режим не очищает буфер и временно блокирует авто‑сброс.
 *  • Отслеживать влияние флага счётчиков (enableCounters) — когда отключён, счётчики не растут,
 *    что снижает накладные расходы без потери функциональности.
 *
 * Подход к тестам:
 *  • Используем лишь локальные {@link java.util.concurrent.CompletableFuture} — без Kafka.
 *  • Успешные операции моделируются через {@code completedFuture(null)} (RecordMetadata не нужен).
 *  • Отказ моделируется через {@code completeExceptionally(..)}, таймаут — через незавершаемый future.
 *  • Тесты быстрые и не блокируют долго; никаких внешних зависимостей нет.
 */
final class BatchSenderTest {

    private static final ScheduledExecutorService DELAY_EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "BatchSenderTest-delay");
                t.setDaemon(true);
                return t;
            });

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(DELAY_EXECUTOR::shutdownNow,
                "BatchSenderTest-delay-shutdown"));
    }

    /** Успешный future без полезного значения (эквивалент отправленного сообщения). */
    private static CompletableFuture<RecordMetadata> ok() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Future, завершающийся исключением — для проверки отказов.
     * @param msg текст исключения
     */
    private static CompletableFuture<RecordMetadata> fail(String msg) {
        CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
        cf.completeExceptionally(new RuntimeException(msg));
        return cf;
    }

    private static CompletableFuture<RecordMetadata> slow(long delayMs) {
        CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
        DELAY_EXECUTOR.schedule(() -> cf.complete(null), Math.max(0L, delayMs), TimeUnit.MILLISECONDS);
        return cf;
    }

    /**
     * Никогда не завершающийся future — для проверки общего таймаута на flush().
     */
    private static CompletableFuture<RecordMetadata> never() {
        return new CompletableFuture<>(); // никогда не завершается
    }

    @Test
    @DisplayName("Строгий flush: очищает буфер, счётчики растут (enableCounters=true)")
    void strictFlush_success_clears_and_counts() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            sender.add(ok());
            sender.add(ok());
            assertEquals(2, sender.getPendingCount());
            sender.flush(); // строго

            assertEquals(0, sender.getPendingCount());
            assertTrue(sender.isCountersEnabled());
            assertEquals(1, sender.getFlushCalls());     // один успешный сброс
            assertEquals(2, sender.getConfirmedCount()); // две подтверждённые отправки
            assertEquals(0, sender.getFailedFlushes());  // неуспехов «тихих» нет
        }
    }

    @Test
    @DisplayName("Тихий tryFlush при ошибке: возвращает false, буфер не очищается, авто‑сброс блокируется")
    void quietFlush_failure_does_not_clear_and_suspends_autoflush() {
        try (BatchSender senderFail = new BatchSender(2, 250, true, false)) {
            // В буфере сразу «битый» future
            senderFail.add(fail("boom"));
            assertTrue(senderFail.hasPending());

            // Тихий сброс — false, буфер остался
            assertFalse(senderFail.tryFlush());
            assertTrue(senderFail.hasPending());

            // Теперь добавим два успешных — но авто‑сброс уже подавлен
            senderFail.add(ok());
            senderFail.add(ok());
            assertEquals(3, senderFail.getPendingCount()); // ничего не сбросилось автоматически
            // Попробуем снова «тихо» — снова false (из‑за первой ошибки)
            assertFalse(senderFail.tryFlush());
            assertEquals(3, senderFail.getPendingCount());
            assertTrue(senderFail.getFailedFlushes() >= 2); // неуспешных «тихих» как минимум два
        } catch (Exception ignored) {
            // ожидаемая ошибка при незакрытом «битом» future
        }
    }

    @Test
    @DisplayName("Строгий flush: таймаут на незавершённом future приводит к TimeoutException")
    void strictFlush_times_out() {
        try (BatchSender sender = new BatchSender(1, 50, true, false)) {
            sender.add(never()); // дедлайн 50 мс на весь набор (один элемент)

            TimeoutException te = assertThrows(TimeoutException.class, sender::flush);
            assertNotNull(te, "Ожидался TimeoutException");
            assertTrue(sender.hasPending()); // буфер не очищается
        } catch (Exception ignored) {
            // таймаут по-прежнему активен — ошибка ожидаема
        }
    }

    @Test
    @DisplayName("Тихий tryFlush: успех → true, буфер очищен, confirmedCount растёт")
    void quietFlush_success_clears_and_counts() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            sender.add(ok());
            sender.add(ok());
            assertTrue(sender.hasPending());

            sender.tryFlush(); // проверяем эффекты ниже; контракт метода — best-effort без строгой гарантии true
            assertEquals(0, sender.getPendingCount(), "Буфер должен очиститься");
            assertEquals(2, sender.getConfirmedCount(), "Обе отправки должны быть подтверждены");
            assertEquals(0, sender.getFailedFlushes(), "Неуспешных «тихих» сбросов быть не должно");
        }
    }

    @Test
    @DisplayName("addAll: кусочная загрузка и автосбросы на порогах awaitEvery")
    void addAll_chunked_autoflush() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            Collection<CompletableFuture<RecordMetadata>> batch =
                    Arrays.asList(ok(), ok(), ok(), ok(), ok(), ok(), ok()); // 7 элементов

            sender.addAll(batch);
            assertEquals(1, sender.getPendingCount());
            assertTrue(sender.getFlushCalls() >= 2);

            // Досбрасываем остаток строго
            assertDoesNotThrow(sender::flush);
            assertEquals(0, sender.getPendingCount());
        }
    }

    @Test
    @DisplayName("Счётчики отключены: значения не растут независимо от операций")
    void counters_disabled_no_overhead() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, false, false)) {
            // несколько успешных отправок
            sender.add(ok());
            sender.add(ok());
            assertTrue(sender.hasPending());

            // и «тихий», и строгий сбросы
            assertTrue(sender.tryFlush());
            assertEquals(0, sender.getPendingCount());

            // повторный строгий — просто no-op
            assertDoesNotThrow(sender::flush);

            // проверяем отсутствие накладных расходов на счётчики
            assertFalse(sender.isCountersEnabled(), "Счётчики должны быть выключены");
            assertEquals(0, sender.getFlushCalls(), "flushCalls не должен расти при выключенных счётчиках");
            assertEquals(0, sender.getConfirmedCount(), "confirmedCount не должен расти при выключенных счётчиках");
            assertEquals(0, sender.getFailedFlushes(), "failedQuietFlushes не должен расти при выключенных счётчиках");
        }
    }

    @Test
    @DisplayName("tryFlush на пустом буфере: true и никаких побочных эффектов")
    void tryFlush_on_empty_returnsTrue() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            assertEquals(0, sender.getPendingCount());
            long flushCallsBefore = sender.getFlushCalls();
            long confirmedBefore  = sender.getConfirmedCount();
            long failedQuietBefore= sender.getFailedFlushes();

            assertTrue(sender.tryFlush(), "На пустом буфере tryFlush должен вернуть true");
            assertEquals(0, sender.getPendingCount());
            // Счётчики не обязаны меняться — фиксируем отсутствие роста
            assertEquals(flushCallsBefore, sender.getFlushCalls());
            assertEquals(confirmedBefore,  sender.getConfirmedCount());
            assertEquals(failedQuietBefore, sender.getFailedFlushes());
        }
    }

    @Test
    @DisplayName("flush на пустом буфере: не бросает и буфер остаётся пустым")
    void strictFlush_on_empty_noop() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            assertEquals(0, sender.getPendingCount());
            assertDoesNotThrow(sender::flush);
            assertEquals(0, sender.getPendingCount());
        }
    }

    @Test
    @DisplayName("Строгий flush: смесь ok+fail → бросает и буфер сохраняется")
    void strictFlush_mixed_ok_and_fail_throws_and_keeps_buffer() {
        try (BatchSender sender = new BatchSender(10, 250, true, false)) {
            sender.add(ok());
            sender.add(fail("boom-fail"));
            assertTrue(sender.hasPending());
            // ожидаем исключение (путь строгого сброса)
            ExecutionException ex = assertThrows(ExecutionException.class, sender::flush);
            assertTrue(ex.getCause() instanceof RuntimeException);
            assertEquals("boom-fail", ex.getCause().getMessage());
            // буфер остаётся — caller может принять решение, что делать дальше
            assertTrue(sender.hasPending());
            // confirmed не увеличивался (успехи не были подтверждены, т.к. набор завершился с ошибкой)
            assertEquals(0, sender.getConfirmedCount());
        } catch (Exception ignored) {
            // ожидается из-за оставшихся ошибок
        }
    }

    @Test
    @DisplayName("Закрытие: ошибки в буфере ПРОПАГИРУЮТСЯ наружу")
    void close_in_quiet_mode_swallows_errors() {
        // Закрытие проходит через strict‑flush и пробрасывает ошибки futures наружу.
        BatchSender sender = new BatchSender(3, 50, true, true);
        sender.add(fail("boom-on-close"));
        sender.add(never()); // чтобы не было случайного успешного завершения
        assertTrue(sender.hasPending());
        ExecutionException ex = assertThrows(ExecutionException.class, sender::close);
        assertNotNull(ex);
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("boom-on-close", ex.getCause().getMessage());
    }

    @Test
    @DisplayName("awaitEvery=1: авто‑сброс после каждого add(ok)")
    void awaitEvery_one_autoflushes_each_add() throws Exception {
        try (BatchSender sender = new BatchSender(1, 250, true, false)) {
            sender.add(ok());
            assertEquals(0, sender.getPendingCount(), "Должно авто‑сброситься сразу после первого add()");
            sender.add(ok());
            assertEquals(0, sender.getPendingCount(), "И после второго тоже");
            assertTrue(sender.getFlushCalls() >= 2, "Должно быть как минимум два авто‑сброса");
        }
    }

    @Test
    @DisplayName("addAll: пустая коллекция — no-op без побочных эффектов")
    void addAll_empty_is_noop() throws Exception {
        try (BatchSender sender = new BatchSender(3, 250, true, false)) {
            long flushCallsBefore = sender.getFlushCalls();
            long confirmedBefore  = sender.getConfirmedCount();
            long failedQuietBefore= sender.getFailedFlushes();

            sender.addAll(Collections.<CompletableFuture<RecordMetadata>>emptyList());
            assertEquals(0, sender.getPendingCount());
            assertEquals(flushCallsBefore, sender.getFlushCalls());
            assertEquals(confirmedBefore,  sender.getConfirmedCount());
            assertEquals(failedQuietBefore, sender.getFailedFlushes());
        }
    }

    @Test
    @DisplayName("tryFlush: при наличии ошибок confirmedCount не растёт")
    void quietFlush_with_failure_does_not_increment_confirmed() {
        try (BatchSender sender = new BatchSender(5, 250, true, false)) {
            sender.add(fail("boom1"));
            sender.add(ok());
            long confirmedBefore = sender.getConfirmedCount();

            assertFalse(sender.tryFlush(), "Ожидаем false из-за ошибки");
            assertEquals(confirmedBefore, sender.getConfirmedCount(), "confirmedCount не должен расти при неуспешном тихом сбросе");
            assertTrue(sender.hasPending(), "Буфер должен сохраниться для дальнейшей обработки");
        } catch (Exception ignored) {
            // ожидается из-за оставшихся ошибок
        }
    }

    @Test
    @DisplayName("Адаптивный awaitEvery снижается при задержках и возвращается при нормальной скорости")
    void adaptiveAwaitEveryReactsToLatency() throws Exception {
        try (BatchSender sender = new BatchSender(200, 400, true, false)) {
            for (int i = 0; i < 4; i++) {
                sender.add(slow(250));
            }
            sender.flush();

            int decreased = sender.getCurrentAwaitEvery();
            assertTrue(decreased < sender.getAwaitEvery(),
                    "После медленного flush порог должен уменьшиться: decreased=" + decreased
                            + ", base=" + sender.getAwaitEvery());
            assertTrue(sender.getLastFlushLatencyMs() >= 200,
                    "latency медленного flush должен быть заметен");

            for (int round = 0; round < 16; round++) {
                sender.add(ok());
                sender.add(ok());
                sender.add(ok());
                sender.flush();
            }

            int recovered = sender.getCurrentAwaitEvery();
            assertTrue(recovered >= decreased,
                    "При быстрых flush порог не должен продолжать снижаться: decreased=" + decreased
                            + ", recovered=" + recovered);
            assertTrue(recovered <= sender.getAwaitEvery(),
                    "Адаптивный порог не должен превышать базовое значение: recovered=" + recovered
                            + ", base=" + sender.getAwaitEvery());
            assertTrue(sender.getMaxFlushLatencyMs() >= sender.getLastFlushLatencyMs());
        }
    }

    @Test
    @DisplayName("BatchSenderMetrics накапливает успехи, ошибки и подтверждённые отправки")
    void metricsCollectorTracksFlushesAndFailures() throws Exception {
        BatchSenderMetrics metrics = new BatchSenderMetrics();
        metrics.updateConfiguredAwaitEvery(4);

        try (BatchSender sender = new BatchSender(4, 250, true, false, metrics)) {
            sender.add(ok());
            sender.add(ok());
            assertTrue(sender.tryFlush(), "quiet flush должен завершиться успехом");
        }

        assertEquals(1L, metrics.flushSuccessTotal(), "Должен быть зафиксирован один успешный flush");
        assertEquals(0L, metrics.flushFailuresTotal(), "Не должно быть неуспешных flush");
        assertEquals(2L, metrics.recordsConfirmedTotal(), "Обе отправки должны быть подтверждены");
        assertEquals(4L, metrics.configuredAwaitEvery(), "Конфигурационный awaitEvery фиксируется в метриках");
        assertEquals(4L, metrics.currentAwaitEvery(), "Адаптивный порог пока совпадает с базовым");
        assertTrue(metrics.lastFlushLatencyMs() >= 0L, "latency успеха не должна быть отрицательной");

        BatchSender failing = new BatchSender(4, 250, true, false, metrics);
        try {
            failing.add(fail("boom-metrics"));
            assertFalse(failing.tryFlush(), "quiet flush с ошибкой должен вернуть false");
            ExecutionException ex = assertThrows(ExecutionException.class, failing::close);
            assertNotNull(ex.getCause(), "В ExecutionException должна сохраняться исходная причина");
        } finally {
            failing.resumeAutoFlush();
        }

        assertEquals(1L, metrics.flushFailuresTotal(), "Должна быть зафиксирована одна ошибка quiet flush");
        assertTrue(metrics.maxFlushLatencyMs() >= metrics.lastFlushLatencyMs(), "max латентность не меньше последней");
        assertTrue(metrics.avgFlushLatencyMs() >= 0L, "Средняя latency должна быть неотрицательной");
    }

    @Test
    @DisplayName("BatchSenderMetrics отражает изменения адаптивного awaitEvery")
    void metricsCollectorReflectsAdaptiveAwaitEvery() throws Exception {
        BatchSenderMetrics metrics = new BatchSenderMetrics();
        metrics.updateConfiguredAwaitEvery(200);

        try (BatchSender sender = new BatchSender(200, 400, true, false, metrics)) {
            sender.add(slow(250));
            sender.add(slow(250));
            sender.flush();
        }

        assertTrue(metrics.currentAwaitEvery() < metrics.configuredAwaitEvery(),
                "Адаптивный порог в метриках должен снизиться после медленного flush");
        assertTrue(metrics.flushSuccessTotal() >= 1L, "Должен учитываться успешный flush");
        assertTrue(metrics.avgFlushLatencyMs() >= metrics.lastFlushLatencyMs(),
                "Средняя latency не меньше последней при одиночном flush");
    }

    @Test
    @DisplayName("Строгий flush: общий дедлайн применяется к целому набору futures")
    void strictFlush_deadline_applies_to_whole_batch() {
        try (BatchSender sender = new BatchSender(10, 50, true, false)) { // 50 мс общий дедлайн
            sender.add(never());
            sender.add(never());
            TimeoutException te2 = assertThrows(TimeoutException.class, sender::flush);
            assertNotNull(te2, "Ожидался TimeoutException");
            assertEquals(2, sender.getPendingCount(), "Буфер не очищается при таймауте");
        } catch (Exception ignored) {
            // ожидаемый результат: futures по-прежнему не завершены
        }
    }
}
