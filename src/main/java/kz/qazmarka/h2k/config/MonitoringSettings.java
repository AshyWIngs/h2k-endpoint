package kz.qazmarka.h2k.config;

/**
 * Настройки мониторинга/диагностики: включение наблюдателей WAL и экспорт JMX‑метрик.
 */
public final class MonitoringSettings {
    private final boolean observersEnabled;
    private final boolean jmxEnabled;

    public MonitoringSettings(boolean observersEnabled, boolean jmxEnabled) {
        this.observersEnabled = observersEnabled;
        this.jmxEnabled = jmxEnabled;
    }

    public boolean isObserversEnabled() {
        return observersEnabled;
    }

    public boolean isJmxEnabled() {
        return jmxEnabled;
    }
}
