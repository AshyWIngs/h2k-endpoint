package kz.qazmarka.h2k.endpoint.internal;

/**
 * Контейнер метаданных WAL, используемый при формировании payload.
 */
public final class WalMeta {
    public static final WalMeta EMPTY = new WalMeta(-1L, -1L);

    public final long seq;
    public final long writeTime;

    public WalMeta(long seq, long writeTime) {
        this.seq = seq;
        this.writeTime = writeTime;
    }
}
