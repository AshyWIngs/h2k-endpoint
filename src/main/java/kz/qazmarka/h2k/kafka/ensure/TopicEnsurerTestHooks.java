package kz.qazmarka.h2k.kafka.ensure;

import kz.qazmarka.h2k.kafka.ensure.metrics.TopicEnsureState;

/**
 * Пакетный вспомогательный класс для сборки {@link TopicEnsurer} в модульных тестах без reflection.
 * Делегирует вызов пакетному конструктору и предоставляется только в test-scope.
 */
final class TopicEnsurerTestHooks {
    private TopicEnsurerTestHooks() {
        // utility
    }

    static TopicEnsurer testingInstance(TopicEnsureService service,
                                        TopicEnsureExecutor executor,
                                        TopicEnsureState state) {
        return new TopicEnsurer(service, executor, state);
    }
}
