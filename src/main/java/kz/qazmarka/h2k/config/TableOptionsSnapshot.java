package kz.qazmarka.h2k.config;

/**
 * Иммутабельный снимок табличных опций: количество байт соли, подсказка ёмкости и фильтр CF.
 * Экземпляр безопасен для публикации между потоками и не содержит изменяемых коллекций.
 */
public final class TableOptionsSnapshot {
    private final int saltBytes;
    private final TableValueSource saltSource;
    private final int capacityHint;
    private final TableValueSource capacitySource;
    private final CfFilterSnapshot cfFilter;

    public TableOptionsSnapshot(int saltBytes,
                                TableValueSource saltSource,
                                int capacityHint,
                                TableValueSource capacitySource,
                                CfFilterSnapshot cfFilter) {
        this.saltBytes = saltBytes;
        this.saltSource = (saltSource == null) ? TableValueSource.DEFAULT : saltSource;
        this.capacityHint = capacityHint;
        this.capacitySource = (capacitySource == null) ? TableValueSource.DEFAULT : capacitySource;
        this.cfFilter = (cfFilter == null) ? CfFilterSnapshot.disabled() : cfFilter;
    }

    public int saltBytes() {
        return saltBytes;
    }

    public TableValueSource saltSource() {
        return saltSource;
    }

    public int capacityHint() {
        return capacityHint;
    }

    public TableValueSource capacitySource() {
        return capacitySource;
    }

    public CfFilterSnapshot cfFilter() {
        return cfFilter;
    }
}
