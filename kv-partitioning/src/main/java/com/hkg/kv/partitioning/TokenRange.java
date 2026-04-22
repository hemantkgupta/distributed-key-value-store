package com.hkg.kv.partitioning;

import java.math.BigInteger;
import java.util.List;

public record TokenRange(Token startExclusive, Token endInclusive) {
    private static final BigInteger TWO_TO_64 = BigInteger.ONE.shiftLeft(64);

    public TokenRange {
        if (startExclusive == null) {
            throw new IllegalArgumentException("start token must not be null");
        }
        if (endInclusive == null) {
            throw new IllegalArgumentException("end token must not be null");
        }
    }

    public static TokenRange fullRing() {
        return new TokenRange(new Token(0L), new Token(0L));
    }

    public boolean isFullRing() {
        return startExclusive.equals(endInclusive);
    }

    public boolean contains(Token token) {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        if (isFullRing()) {
            return true;
        }

        int startToEnd = startExclusive.compareTo(endInclusive);
        if (startToEnd < 0) {
            return token.compareTo(startExclusive) > 0 && token.compareTo(endInclusive) <= 0;
        }
        return token.compareTo(startExclusive) > 0 || token.compareTo(endInclusive) <= 0;
    }

    public boolean canSplit() {
        return unsignedDistance().compareTo(BigInteger.ONE) > 0;
    }

    public List<TokenRange> split() {
        if (!canSplit()) {
            throw new IllegalStateException("token range is too small to split");
        }

        Token midpoint = midpoint();
        return List.of(
                new TokenRange(startExclusive, midpoint),
                new TokenRange(midpoint, endInclusive)
        );
    }

    private Token midpoint() {
        BigInteger start = unsignedLongToBigInteger(startExclusive.value());
        BigInteger halfDistance = unsignedDistance().divide(BigInteger.TWO);
        return new Token(bigIntegerToLong(start.add(halfDistance)));
    }

    private BigInteger unsignedDistance() {
        if (isFullRing()) {
            return TWO_TO_64;
        }

        BigInteger start = unsignedLongToBigInteger(startExclusive.value());
        BigInteger end = unsignedLongToBigInteger(endInclusive.value());
        if (end.compareTo(start) > 0) {
            return end.subtract(start);
        }
        return TWO_TO_64.subtract(start).add(end);
    }

    private static BigInteger unsignedLongToBigInteger(long value) {
        BigInteger result = BigInteger.valueOf(value);
        return value < 0 ? result.add(TWO_TO_64) : result;
    }

    private static long bigIntegerToLong(BigInteger value) {
        return value.mod(TWO_TO_64).longValue();
    }
}
