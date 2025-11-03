package kz.qazmarka.h2k.kafka.ensure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import kz.qazmarka.h2k.kafka.ensure.TopicCandidateChecker.Candidate;
import kz.qazmarka.h2k.kafka.ensure.TopicCandidateChecker.CandidateStatus;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TopicCandidateCheckerTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopicCandidateCheckerTest.class);

    @Test
    @DisplayName("Пустое или пробельное имя отклоняется как SKIP")
    void blankTopicIsRejected() {
           Fixture fx = fixture();
        Candidate candidate = fx.checker.evaluate("   ");

        assertEquals(CandidateStatus.SKIP, candidate.status);
        IllegalStateException thrown = assertThrows(IllegalStateException.class, candidate::normalizedName);
        assertEquals("normalized name is unavailable for status SKIP", thrown.getMessage());
        assertEquals(1L, fx.state.ensureEvaluations().longValue());
        assertEquals(1L, fx.state.ensureRejected().longValue());
    }

    @Test
    @DisplayName("Некорректные символы приводят к SKIP")
    void invalidTopicRejected() {
           Fixture fx = fixture();

        Candidate candidate = fx.checker.evaluate("bad*topic");
        assertEquals(CandidateStatus.SKIP, candidate.status);
        assertEquals(1L, fx.state.ensureEvaluations().longValue());
        assertEquals(1L, fx.state.ensureRejected().longValue());
    }

    @Test
    @DisplayName("Уже подтверждённая тема возвращает ALREADY_ENSURED и увеличивает метрику кеша")
    void alreadyEnsuredHitsCache() {
           Fixture fx = fixture();
        fx.state.markEnsured("orders");

        Candidate candidate = fx.checker.evaluate("  orders ");
        assertEquals(CandidateStatus.ALREADY_ENSURED, candidate.status);
        assertEquals("orders", candidate.normalizedName());
        assertEquals(1L, fx.state.ensureHitCache().longValue());
        assertEquals(1L, fx.state.ensureEvaluations().longValue());
        assertEquals(0L, fx.state.ensureRejected().longValue());
    }

    @Test
    @DisplayName("Активный backoff блокирует повторную проверку")
    void activeBackoffSkips() {
            Fixture fx = fixture();
        fx.state.scheduleUnknown("delayed", System.nanoTime() + TimeUnit.MINUTES.toNanos(1));

        Candidate candidate = fx.checker.evaluate("delayed");
        assertEquals(CandidateStatus.SKIP, candidate.status);
        assertEquals(1L, fx.state.ensureEvaluations().longValue());
        assertEquals(1L, fx.state.ensureRejected().longValue());
    }

    @Test
    @DisplayName("Sanitizer может отклонить тему, вернув null")
    void sanitizerNullProducesSkip() {
        Fixture fx = fixture(raw -> null);

        Candidate candidate = fx.checker.evaluate("any");
        assertEquals(CandidateStatus.SKIP, candidate.status);
        assertEquals(1L, fx.state.ensureEvaluations().longValue());
        assertEquals(1L, fx.state.ensureRejected().longValue());
    }

    @Test
    @DisplayName("Валидная тема нормализуется и принимается")
    void validTopicAccepted() {
        Fixture fx = fixture(raw -> raw == null ? null : raw.trim().toUpperCase());

        Candidate candidate = fx.checker.evaluate("  Foo-Bar ");
        assertEquals(CandidateStatus.ACCEPTED, candidate.status);
        assertEquals("FOO-BAR", candidate.normalizedName());
        assertEquals(1L, fx.state.ensureEvaluations().longValue());
        assertEquals(0L, fx.state.ensureRejected().longValue());
    }

    @Test
    @DisplayName("select возвращает только валидные темы без дублей")
    void selectFiltersCollection() {
            Fixture fx = fixture();
        fx.state.scheduleUnknown("bar", System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
        Collection<String> source = Arrays.asList("foo", "bad!", "foo ", "   ", "bar");

        LinkedHashSet<String> accepted = fx.checker.select(source);

        assertEquals(1, accepted.size());
        assertTrue(accepted.contains("foo"));
        assertEquals(5L, fx.state.ensureEvaluations().longValue());
        assertEquals(3L, fx.state.ensureRejected().longValue());
    }

        private static Fixture fixture() {
            return fixture(trimSanitizer());
        }

    private static Fixture fixture(UnaryOperator<String> sanitizer) {
        EnsureRuntimeState state = new EnsureRuntimeState();
        TopicEnsureConfig config = TopicEnsureConfig.builder()
                .topicNameMaxLen(32)
                .topicSanitizer(sanitizer)
                .topicPartitions(1)
                .topicReplication((short) 1)
                .topicConfigs(Collections.emptyMap())
                .ensureIncreasePartitions(false)
                .ensureDiffConfigs(false)
                .adminTimeoutMs(1_000L)
                .unknownBackoffMs(500L)
                .build();
        TopicBackoffManager backoff = new TopicBackoffManager(state, config.unknownBackoffMs());
        EnsureMetricsSink metrics = new EnsureMetricsSink(state);
        TopicCandidateChecker checker = new TopicCandidateChecker(config, state, backoff, metrics, LOG);
        return new Fixture(checker, state);
    }

    private static UnaryOperator<String> trimSanitizer() {
        return raw -> raw == null ? null : raw.trim();
    }

    private static final class Fixture {
        final TopicCandidateChecker checker;
        final EnsureRuntimeState state;

        Fixture(TopicCandidateChecker checker, EnsureRuntimeState state) {
            this.checker = checker;
            this.state = state;
        }
    }
}
