package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.avro.Schema;
import org.slf4j.Logger;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Управляет повторными попытками регистрации схемы в Schema Registry.
 * Выделен в отдельный класс, чтобы разгрузить основной сериализатор и упростить тестирование логики бэкоффа.
 */
final class SchemaRegistrationRetrier implements AutoCloseable {

    private final Logger log;
    private final SchemaRegistryAccess registry;
    private final SchemaFingerprintMonitor fingerprintMonitor;
    private final LongAdder successCounter;
    private final LongAdder failureCounter;
    private final ConfluentAvroPayloadSerializer.RetrySettings settings;
    private final ScheduledExecutorService executor;

    interface RetryCallback {
        void onSchemaRegistered(String subject, Schema schema, int schemaId);
    }

    SchemaRegistrationRetrier(Logger log,
                              SchemaRegistryAccess registry,
                              SchemaFingerprintMonitor fingerprintMonitor,
                              LongAdder successCounter,
                              LongAdder failureCounter,
                              ConfluentAvroPayloadSerializer.RetrySettings settings) {
        this.log = Objects.requireNonNull(log, "log");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.fingerprintMonitor = Objects.requireNonNull(fingerprintMonitor, "fingerprintMonitor");
        this.successCounter = Objects.requireNonNull(successCounter, "successCounter");
        this.failureCounter = Objects.requireNonNull(failureCounter, "failureCounter");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "h2k-schema-registry-retry");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Планирует повторную регистрацию схемы с первой задержкой согласно настройкам бэкоффа.
     *
     * @param subject ключ Schema Registry
     * @param schema локальная Avro-схема, которую требуется зарегистрировать
     * @param fingerprint локальный fingerprint, используемый для последующего кеширования id
     * @param callback действие, выполняемое при успешной регистрации
     */
    void schedule(String subject, Schema schema, long fingerprint, RetryCallback callback) {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(callback, "callback");
        long delay = settings.delayMsForAttempt(1);
        executor.schedule(() -> attempt(subject, schema, fingerprint, 1, callback), delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Выполняет одну попытку регистрации схемы и при необходимости переназначает следующую.
     */
    private void attempt(String subject,
                         Schema schema,
                         long fingerprint,
                         int attempt,
                         RetryCallback callback) {
        try {
            int schemaId = registry.register(subject, schema);
            successCounter.increment();
            callback.onSchemaRegistered(subject, schema, schemaId);
            fingerprintMonitor.recordSuccessfulRegistration(subject, fingerprint, schemaId);
            log.info("Avro Confluent: схема subject={} успешно зарегистрирована после {} попыток", subject, attempt);
        } catch (RestClientException ex) {
            failureCounter.increment();
            if (!SchemaRegistryErrors.isRetryable(ex)) {
                log.error("Avro Confluent: повторная регистрация схемы subject={} прекращена: {}",
                        subject, SchemaRegistryErrors.summary(ex));
                return;
            }
            if (attempt >= settings.maxAttempts()) {
                log.error("Avro Confluent: регистрация схемы subject={} окончательно провалилась после {} попыток: {}",
                        subject, attempt, SchemaRegistryErrors.summary(ex));
                return;
            }
            log.warn("Avro Confluent: повторная регистрация схемы subject={} не удалась (попытка {}): {}",
                    subject, attempt, SchemaRegistryErrors.summary(ex));
            long delay = settings.delayMsForAttempt(attempt + 1);
            executor.schedule(() -> attempt(subject, schema, fingerprint, attempt + 1, callback), delay, TimeUnit.MILLISECONDS);
        } catch (IOException ex) {
            failureCounter.increment();
            if (attempt >= settings.maxAttempts()) {
                log.error("Avro Confluent: регистрация схемы subject={} окончательно провалилась после {} попыток: {}",
                        subject, attempt, ex.getMessage());
                return;
            }
            log.warn("Avro Confluent: повторная регистрация схемы subject={} не удалась (попытка {}): {}",
                    subject, attempt, ex.getMessage());
            long delay = settings.delayMsForAttempt(attempt + 1);
            executor.schedule(() -> attempt(subject, schema, fingerprint, attempt + 1, callback), delay, TimeUnit.MILLISECONDS);
        } catch (RuntimeException ex) {
            failureCounter.increment();
            log.error("Avro Confluent: критическая ошибка при повторной регистрации схемы subject={}", subject, ex);
        }
    }

    /**
     * Принудительно останавливает планировщик повторных регистраций и отменяет будущие задачи.
     */
    @Override
    public void close() {
        executor.shutdownNow();
    }
}
