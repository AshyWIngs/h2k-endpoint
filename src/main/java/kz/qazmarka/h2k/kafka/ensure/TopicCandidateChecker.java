package kz.qazmarka.h2k.kafka.ensure;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.function.Function;

import org.slf4j.Logger;

import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Проверяет кандидатов на ensure: нормализация имён, валидация,
 * проверка кеша и backoff.
 */
final class TopicCandidateChecker {

    private static final String WARN_EMPTY_TOPIC = "Пустое имя Kafka-топика — пропускаю ensure";
    private static final String WARN_INVALID_TOPIC =
            "Некорректное имя Kafka-топика '{}': допускаются [a-zA-Z0-9._-], длина 1..{}, запрещены '.' и '..'";

    private final TopicEnsureConfig config;
    private final TopicEnsureState state;
    private final TopicBackoffManager backoffManager;
    private final Logger log;
    private final Function<String, String> sanitizer;

    TopicCandidateChecker(TopicEnsureConfig config,
                          TopicEnsureState state,
                          TopicBackoffManager backoffManager,
                          Logger log) {
        this.config = config;
        this.state = state;
        this.backoffManager = backoffManager;
        this.log = log;
        this.sanitizer = config.topicSanitizer();
    }

    Candidate evaluate(String rawTopic) {
        String normalized = normalize(rawTopic);
        if (normalized.isEmpty()) {
            log.warn(WARN_EMPTY_TOPIC);
            return Candidate.skip();
        }
        if (!TopicNameValidator.isValid(normalized, config.topicNameMaxLen())) {
            log.warn(WARN_INVALID_TOPIC, normalized, config.topicNameMaxLen());
            return Candidate.skip();
        }
        if (state.ensured.contains(normalized)) {
            state.ensureHitCache.increment();
            if (log.isDebugEnabled()) {
                log.debug("Kafka-топик '{}' уже проверен ранее — пропускаю ensure", normalized);
            }
            return Candidate.alreadyEnsured(normalized);
        }
        if (backoffManager.shouldSkip(normalized)) {
            return Candidate.skip();
        }
        return Candidate.accepted(normalized);
    }

    LinkedHashSet<String> select(Collection<String> topics) {
        LinkedHashSet<String> accepted = new LinkedHashSet<>(topics.size());
        for (String raw : topics) {
            Candidate candidate = evaluate(raw);
            if (candidate.status == CandidateStatus.ACCEPTED) {
                accepted.add(candidate.normalizedName());
            }
        }
        return accepted;
    }

    private String normalize(String topic) {
        String base = topic == null ? "" : topic.trim();
        String sanitized = sanitizer.apply(base);
        return sanitized == null ? "" : sanitized;
    }

    enum CandidateStatus {
        ACCEPTED,
        ALREADY_ENSURED,
        SKIP
    }

    static final class Candidate {
        final CandidateStatus status;
        private final String normalizedName;

        private Candidate(CandidateStatus status, String normalizedName) {
            this.status = status;
            this.normalizedName = normalizedName;
        }

        static Candidate accepted(String normalizedName) {
            return new Candidate(CandidateStatus.ACCEPTED, normalizedName);
        }

        static Candidate alreadyEnsured(String normalizedName) {
            return new Candidate(CandidateStatus.ALREADY_ENSURED, normalizedName);
        }

        static Candidate skip() {
            return new Candidate(CandidateStatus.SKIP, null);
        }

        String normalizedName() {
            if (normalizedName == null) {
                throw new IllegalStateException("normalized name is unavailable for status " + status);
            }
            return normalizedName;
        }
    }
}
