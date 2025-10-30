package kz.qazmarka.h2k.kafka.ensure;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Координирует describe/create ensure-цепочки и обновляет метрики.
 */
final class EnsureCoordinator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EnsureCoordinator.class);

    static final String CFG_RETENTION_MS = "retention.ms";
    static final String CFG_CLEANUP_POLICY = "cleanup.policy";
    static final String CFG_COMPRESSION_TYPE = "compression.type";
    static final String CFG_MIN_INSYNC_REPLICAS = "min.insync.replicas";

    private final KafkaTopicAdmin admin;
    private final TopicEnsureConfig config;
    private final EnsureRuntimeState state;
    private final long adminTimeoutMs;
    private final long unknownBackoffMs;
    private final TopicBackoffManager backoffManager;
    private final EnsureMetricsSink metrics;
    private final TopicDescribeSupport describeSupport;
    private final TopicCreationSupport creationSupport;
    private final TopicCandidateChecker candidateChecker;

    EnsureCoordinator(KafkaTopicAdmin admin, TopicEnsureConfig config, EnsureRuntimeState state) {
        this.admin = admin;
        this.config = config;
        this.state = state;
        this.adminTimeoutMs = config.adminTimeoutMs();
        this.unknownBackoffMs = config.unknownBackoffMs();
        this.backoffManager = new TopicBackoffManager(state, config.unknownBackoffMs());
        Map<String, String> topicConfigs = config.topicConfigs();
        this.metrics = new EnsureMetricsSink(state);
        TopicEnsureContext ctx = new TopicEnsureContext(backoffManager, adminTimeoutMs, topicConfigs, this::markEnsured, metrics, LOG);
        this.describeSupport = new TopicDescribeSupport(admin, ctx);
        this.creationSupport = new TopicCreationSupport(admin, config, ctx);
        this.candidateChecker = new TopicCandidateChecker(config, state, backoffManager, metrics, LOG);
    }

    void ensureTopic(String topic) {
        if (admin == null) {
            return;
        }

        TopicCandidateChecker.Candidate candidate = candidateChecker.evaluate(topic);
        if (candidate.status == TopicCandidateChecker.CandidateStatus.ALREADY_ENSURED) {
            return;
        }
        if (candidate.status != TopicCandidateChecker.CandidateStatus.ACCEPTED) {
            return;
        }
        String normalized = candidate.normalizedName();
        metrics.recordInvocationAccepted();
        ensureAcceptedTopic(normalized);
    }

    boolean ensureTopicOk(String topic) {
        if (admin == null) {
            return false;               // ensureTopics=false
        }
        TopicCandidateChecker.Candidate candidate = candidateChecker.evaluate(topic);
        if (candidate.status == TopicCandidateChecker.CandidateStatus.ALREADY_ENSURED) {
            return true;
        }
        if (candidate.status != TopicCandidateChecker.CandidateStatus.ACCEPTED) {
            return false;
        }
        String normalized = candidate.normalizedName();
        metrics.recordInvocationAccepted();
        ensureAcceptedTopic(normalized);
        return state.isEnsured(normalized);
    }

    void ensureTopics(Collection<String> topics) {
        if (admin == null || topics == null || topics.isEmpty()) return;
        Set<String> toCheck = candidateChecker.select(topics);
        if (toCheck.isEmpty()) return;
        toCheck.forEach(topic -> metrics.recordInvocationAccepted());
        metrics.recordBatchAccepted(toCheck.size());
        List<String> missing = describeSupport.describeMissing(toCheck);
        if (missing.isEmpty()) return;
        creationSupport.createMissingTopics(missing);
    }

    Map<String, Long> getMetrics() {
        Map<String, Long> m = new LinkedHashMap<>(16);
        m.put("ensure.invocations.total", state.ensureEvaluations().longValue());
        m.put("ensure.invocations.accepted", state.ensureInvocations().longValue());
        m.put("ensure.invocations.rejected", state.ensureRejected().longValue());
        m.put("ensure.invocations", state.ensureInvocations().longValue());
        m.put("ensure.cache.hit",   state.ensureHitCache().longValue());
        m.put("ensure.batch.count", state.ensureBatchCount().longValue());
        m.put("exists.true",        state.existsTrue().longValue());
        m.put("exists.false",       state.existsFalse().longValue());
        m.put("exists.unknown",     state.existsUnknown().longValue());
        m.put("create.ok",          state.createOk().longValue());
        m.put("create.race",        state.createRace().longValue());
        m.put("create.fail",        state.createFail().longValue());
        m.put("unknown.backoff.size", (long) state.unknownSize());
        return Collections.unmodifiableMap(m);
    }

    Map<String, Long> getBackoffSnapshot() {
        final Map<String, Long> pending = state.snapshotUnknown();
        if (pending.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, Long> snapshot = new LinkedHashMap<>(pending.size());
        final long now = System.nanoTime();
        for (Map.Entry<String, Long> e : pending.entrySet()) {
            long remainingNs = e.getValue() - now;
            if (remainingNs < 0L) remainingNs = 0L;
            snapshot.put(e.getKey(), TimeUnit.NANOSECONDS.toMillis(remainingNs));
        }
        return Collections.unmodifiableMap(snapshot);
    }

    EnsureRuntimeState state() {
        return state;
    }

    @Override public void close() {
        if (admin != null) {
            if (LOG.isDebugEnabled()) LOG.debug("Закрываю Kafka AdminClient");
            admin.close(Duration.ofMillis(adminTimeoutMs));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        sb.append("TopicEnsurer{")
          .append("partitions=").append(config.topicPartitions())
          .append(", replication=").append(config.topicReplication())
          .append(", adminTimeoutMs=").append(adminTimeoutMs)
          .append(", unknownBackoffMs=").append(unknownBackoffMs)
          .append(", ensured.size=").append(state.ensuredSize())
          .append(", metrics={")
          .append("ensure=").append(state.ensureInvocations().longValue())
          .append(", hit=").append(state.ensureHitCache().longValue())
          .append(", existsT=").append(state.existsTrue().longValue())
          .append(", existsF=").append(state.existsFalse().longValue())
          .append(", existsU=").append(state.existsUnknown().longValue())
          .append(", createOk=").append(state.createOk().longValue())
          .append(", createRace=").append(state.createRace().longValue())
          .append(", createFail=").append(state.createFail().longValue())
          .append("}}");
        return sb.toString();
    }

    private void ensureAcceptedTopic(String topic) {
        TopicExistence ex = describeSupport.describeSingle(topic);
        switch (ex) {
            case TRUE:
                creationSupport.ensureUpgrades(topic);
                break;
            case UNKNOWN:
                // повторная попытка назначена в describeSupport
                break;
            case FALSE:
                creationSupport.ensureTopicCreated(topic);
                break;
        }
    }

    private void markEnsured(String topic) {
        state.markEnsured(topic);
        backoffManager.markSuccess(topic);
    }
}
