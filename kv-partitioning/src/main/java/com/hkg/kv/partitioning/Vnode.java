package com.hkg.kv.partitioning;

public record Vnode(Token token, ClusterNode owner, int index) implements Comparable<Vnode> {
    public Vnode {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        if (owner == null) {
            throw new IllegalArgumentException("owner must not be null");
        }
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }
    }

    @Override
    public int compareTo(Vnode other) {
        int tokenComparison = token.compareTo(other.token);
        if (tokenComparison != 0) {
            return tokenComparison;
        }

        int ownerComparison = owner.nodeId().value().compareTo(other.owner.nodeId().value());
        if (ownerComparison != 0) {
            return ownerComparison;
        }

        return Integer.compare(index, other.index);
    }
}
