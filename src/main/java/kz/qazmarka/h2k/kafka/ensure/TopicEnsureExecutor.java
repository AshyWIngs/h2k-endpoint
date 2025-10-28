package kz.qazmarka.h2k.kafka.ensure;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Простой исполнитель ensure-задач: одна очередь с задержкой, один рабочий поток.
 * Внешний код только ставит топики в очередь, горячий путь не блокируется.
 */
final class TopicEnsureExecutor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TopicEnsureExecutor.class);

    private final EnsureCoordinator coordinator;
    private final EnsureRuntimeState state;
    private final BlockingQueue<EnsureTask> queue = new DelayQueue<>();
    private final ConcurrentMap<String, EnsureTask> scheduled = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Thread worker;

    TopicEnsureExecutor(EnsureCoordinator coordinator,
                        String threadName) {
        this.coordinator = coordinator;
        this.state = coordinator.state();
        this.worker = new Thread(this::runLoop, threadName);
        this.worker.setDaemon(true);
        start();
    }

    /**
     * Запускает рабочий поток. Должен быть вызван после создания экземпляра.
     */
    void start() {
        if (started.compareAndSet(false, true)) {
            this.worker.start();
        }
    }

    /**
     * Ставит тему в очередь ensure. Возвращает true, если задача действительно была добавлена.
     */
    boolean submit(String topic) {
        if (!isRunning()) {
            return false;
        }
        if (isBlank(topic) || topicAlreadyEnsured(topic)) {
            return false;
        }
        return schedule(topic, computeReadyAt(topic));
    }

    /**
     * @return количество тем, ожидающих ensure в очереди.
     */
    int queuedTopics() {
        return scheduled.size();
    }

    private void runLoop() {
        while (isRunning()) {
            try {
                EnsureTask task = queue.take();
                if (shouldSkipTask(task)) {
                    continue;
                }
                processTask(task);
            } catch (InterruptedException interrupted) {
                handleInterruption();
            }
        }
    }

    private boolean shouldSkipTask(EnsureTask task) {
        if (task == null) {
            return true;
        }
        return !scheduled.remove(task.topic, task) || topicAlreadyEnsured(task.topic);
    }

    private void processTask(EnsureTask task) {
        if (shouldRescheduleTask(task)) {
            return;
        }
        executeEnsure(task.topic);
        rescheduleIfNeeded(task.topic);
    }

    private boolean shouldRescheduleTask(EnsureTask task) {
        long readyAt = computeReadyAt(task.topic);
        long now = System.nanoTime();
        if (readyAt > now) {
            schedule(task.topic, readyAt);
            return true;
        }
        return false;
    }

    private void executeEnsure(String topic) {
        try {
            coordinator.ensureTopic(topic);
        } catch (RuntimeException ex) {
            LOG.warn("Ensure-поток: ошибка при обработке темы '{}': {}", topic, ex.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ensure-поток: трассировка ошибки '{}'", topic, ex);
            }
        }
    }

    private void rescheduleIfNeeded(String topic) {
        if (topicAlreadyEnsured(topic)) {
            return;
        }
        Long nextDeadline = state.getUnknownDeadline(topic);
        if (nextDeadline != null) {
            schedule(topic, nextDeadline);
        }
    }

    private void handleInterruption() {
        if (!isRunning()) {
            Thread.currentThread().interrupt();
        }
    }

    private long computeReadyAt(String topic) {
        Long deadline = state.getUnknownDeadline(topic);
        long now = System.nanoTime();
        return (deadline == null || deadline <= now) ? now : deadline;
    }

    private boolean schedule(String topic, long readyAtNs) {
        if (!isRunning()) {
            return false;
        }
        long now = System.nanoTime();
        long normalized = Math.max(readyAtNs, now);
        EnsureTask candidate = new EnsureTask(topic, normalized);
        return addOrReplaceTask(topic, candidate, normalized);
    }

    private boolean addOrReplaceTask(String topic, EnsureTask candidate, long normalized) {
        while (true) {
            EnsureTask existing = scheduled.putIfAbsent(topic, candidate);
            if (existing == null) {
                return enqueueTask(topic, candidate);
            }
            if (existing.readyAtNs <= normalized) {
                return false;
            }
            if (replaceExistingTask(topic, existing, candidate)) {
                return true;
            }
        }
    }

    private boolean replaceExistingTask(String topic, EnsureTask existing, EnsureTask candidate) {
        if (!scheduled.replace(topic, existing, candidate)) {
            return false;
        }
        removeOldTask(topic, existing);
        return enqueueTask(topic, candidate);
    }

    private void removeOldTask(String topic, EnsureTask existing) {
        boolean removed = queue.remove(existing);
        if (!removed && LOG.isDebugEnabled()) {
            LOG.debug("Не удалось удалить старую задачу из очереди для топика '{}'", topic);
        }
    }

    private boolean enqueueTask(String topic, EnsureTask candidate) {
        if (queue.offer(candidate)) {
            return true;
        }
        LOG.warn("Не удалось добавить задачу в очередь для топика '{}'", topic);
        scheduled.remove(topic, candidate);
        return false;
    }

    private boolean isRunning() {
        return running.get();
    }

    private boolean topicAlreadyEnsured(String topic) {
        return state.isEnsured(topic);
    }

    private static boolean isBlank(String topic) {
        return topic == null || topic.isEmpty();
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) {
            return;
        }
        if (!started.get()) {
            return;
        }
        worker.interrupt();
        try {
            worker.join(TimeUnit.SECONDS.toMillis(1));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            queue.clear();
            scheduled.clear();
        }
    }

    private static final class EnsureTask implements Delayed {
        final String topic;
        final long readyAtNs;

        EnsureTask(String topic, long readyAtNs) {
            this.topic = topic;
            this.readyAtNs = readyAtNs;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = readyAtNs - System.nanoTime();
            if (diff <= 0L) {
                return 0L;
            }
            return unit.convert(diff, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            EnsureTask that = (EnsureTask) other;
            return Long.compare(this.readyAtNs, that.readyAtNs);
        }
    }
}
