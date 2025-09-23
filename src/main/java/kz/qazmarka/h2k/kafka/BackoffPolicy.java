package kz.qazmarka.h2k.kafka;

import java.security.SecureRandom;

/**
 * Простая политика backoff с криптографическим джиттером без дополнительной логики.
 */
final class BackoffPolicy {
    private static final SecureRandom SR = new SecureRandom();
    private final long minNanos;
    private final int jitterPercent;

    BackoffPolicy(long minNanos, int jitterPercent) {
        this.minNanos = minNanos;
        this.jitterPercent = jitterPercent;
    }

    long nextDelayNanos(long baseNanos) {
        long jitter = Math.max(1L, (baseNanos * jitterPercent) / 100L);
        long delta = nextLongBetweenSecure(-jitter, jitter + 1);
        long d = baseNanos + delta;
        return (d < minNanos) ? minNanos : d;
    }

    private static long nextLongBetweenSecure(long originInclusive, long boundExclusive) {
        long n = boundExclusive - originInclusive;
        if (n <= 0) return originInclusive;
        long bits;
        long val;
        do {
            bits = SR.nextLong() >>> 1;
            val = bits % n;
        } while (bits - val + (n - 1) < 0L);
        return originInclusive + val;
    }
}
