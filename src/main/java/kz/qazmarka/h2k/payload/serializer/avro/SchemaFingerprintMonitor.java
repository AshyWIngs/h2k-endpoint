package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.slf4j.Logger;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Отвечает за сравнение локальных и удалённых fingerprint Avro-схем.
 * Выделен, чтобы разгрузить сериализатор и упростить дальнейшее расширение логики мониторинга.
 */
final class SchemaFingerprintMonitor {

    private static final String SUBJECT_PARAM = "subject";

    private final Logger log;
    private final SchemaRegistryAccess registry;
    private final LongAdder successCounter;
    private final ConcurrentHashMap<String, RemoteSchemaFingerprint> fingerprints = new ConcurrentHashMap<>();
    private final AtomicLong lastRecordedFingerprint = new AtomicLong(0);

    SchemaFingerprintMonitor(Logger log,
                             SchemaRegistryAccess registry,
                             LongAdder successCounter) {
        this.log = Objects.requireNonNull(log, "log");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.successCounter = Objects.requireNonNull(successCounter, "successCounter");
    }

    /**
     * Сравнивает локальный fingerprint с удалённым состоянием Schema Registry и логирует расхождения.
     * Успешные обращения к SR учитываются в счётчике.
     */
    void observeRemoteFingerprint(String subject, long localFingerprint) {
    Objects.requireNonNull(subject, SUBJECT_PARAM);
        try {
            SchemaMetadata metadata = registry.latest(subject);
            if (metadata == null) {
                successCounter.increment();
                return;
            }
            long remoteFingerprint = computeRemoteFingerprint(metadata);
            RemoteSchemaFingerprint previous = fingerprints.put(subject,
                    new RemoteSchemaFingerprint(metadata.getId(), metadata.getVersion(), remoteFingerprint));
            successCounter.increment();
            if (remoteFingerprint != localFingerprint) {
                log.warn("Avro Confluent: локальная схема subject={} имеет fingerprint {}, тогда как удалённая {} (version={})",
                        subject, localFingerprint, remoteFingerprint, metadata.getVersion());
            }
            if (previous != null && previous.fingerprint != remoteFingerprint) {
                log.warn("Avro Confluent: fingerprint схемы subject={} изменился: remote={} → {} (id {} → {}, version {} → {})",
                        subject,
                        previous.fingerprint,
                        remoteFingerprint,
                        previous.schemaId,
                        metadata.getId(),
                        previous.version,
                        metadata.getVersion());
            }
        } catch (RestClientException | IOException | RuntimeException ex) {
            if (log.isWarnEnabled()) {
                log.warn("Avro Confluent: не удалось сравнить fingerprint subject={}: {}", subject, ex.getMessage());
            }
            if (log.isDebugEnabled()) {
                log.debug("Трассировка ошибки сравнения fingerprint для subject={}", subject, ex);
            }
        }
    }

    /**
     * Сохраняет fingerprint, полученный при успешной регистрации схемы.
     */
    void recordSuccessfulRegistration(String subject, long localFingerprint, int schemaId) {
        Objects.requireNonNull(subject, SUBJECT_PARAM);
        fingerprints.put(subject, new RemoteSchemaFingerprint(schemaId, -1, localFingerprint));
        this.lastRecordedFingerprint.set(localFingerprint);
    }

    /**
     * Возвращает сведения о ранее известном fingerprint для subject.
     */
    RemoteSchemaFingerprint knownFingerprint(String subject) {
        Objects.requireNonNull(subject, SUBJECT_PARAM);
        return fingerprints.get(subject);
    }

    /**
     * Возвращает последний успешно зарегистрированный fingerprint.
     * Используется для экспонирования метрик через JMX.
     */
    long lastRecordedFingerprint() {
        return lastRecordedFingerprint.get();
    }

    private long computeRemoteFingerprint(SchemaMetadata metadata) {
        Schema parsed = new Schema.Parser().parse(metadata.getSchema());
        return SchemaNormalization.parsingFingerprint64(parsed);
    }

    static final class RemoteSchemaFingerprint {
        final int schemaId;
        final int version;
        final long fingerprint;

        RemoteSchemaFingerprint(int schemaId, int version, long fingerprint) {
            this.schemaId = schemaId;
            this.version = version;
            this.fingerprint = fingerprint;
        }
    }
}
