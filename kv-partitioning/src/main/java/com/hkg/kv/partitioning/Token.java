package com.hkg.kv.partitioning;

public record Token(long value) implements Comparable<Token> {
    @Override
    public int compareTo(Token other) {
        return Long.compareUnsigned(value, other.value);
    }
}
