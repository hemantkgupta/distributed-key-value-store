package com.hkg.kv.partitioning;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record PartitionRingSnapshot(long epoch, int vnodeCountPerNode, List<ClusterNode> nodes, List<Vnode> vnodes) {
    public PartitionRingSnapshot {
        if (vnodeCountPerNode <= 0) {
            throw new IllegalArgumentException("vnode count must be positive");
        }
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (vnodes == null || vnodes.isEmpty()) {
            throw new IllegalArgumentException("vnodes must not be empty");
        }

        for (ClusterNode node : nodes) {
            if (node == null) {
                throw new IllegalArgumentException("nodes must not contain null entries");
            }
        }
        for (Vnode vnode : vnodes) {
            if (vnode == null) {
                throw new IllegalArgumentException("vnodes must not contain null entries");
            }
        }

        nodes = List.copyOf(nodes);
        ArrayList<Vnode> sortedVnodes = new ArrayList<>(vnodes);
        sortedVnodes.sort(Vnode::compareTo);
        vnodes = List.copyOf(sortedVnodes);
    }

    public static PartitionRingSnapshot of(List<ClusterNode> nodes, int vnodeCountPerNode, long epoch) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (vnodeCountPerNode <= 0) {
            throw new IllegalArgumentException("vnode count must be positive");
        }

        LinkedHashMap<NodeId, ClusterNode> distinctNodes = new LinkedHashMap<>();
        for (ClusterNode node : nodes) {
            if (node == null) {
                throw new IllegalArgumentException("nodes must not contain null entries");
            }
            ClusterNode previous = distinctNodes.put(node.nodeId(), node);
            if (previous != null) {
                throw new IllegalArgumentException("node ids must be distinct");
            }
        }

        ArrayList<Vnode> ring = new ArrayList<>(distinctNodes.size() * vnodeCountPerNode);
        for (ClusterNode node : distinctNodes.values()) {
            for (int index = 0; index < vnodeCountPerNode; index++) {
                ring.add(new Vnode(KeyTokenHasher.vnodeTokenFor(node, index), node, index));
            }
        }
        ring.sort(Vnode::compareTo);

        return new PartitionRingSnapshot(epoch, vnodeCountPerNode, new ArrayList<>(distinctNodes.values()), ring);
    }

    public Token tokenFor(Key key) {
        return KeyTokenHasher.tokenFor(key);
    }

    public List<ClusterNode> replicasFor(Key key, int replicationFactor) {
        return replicasFor(tokenFor(key), replicationFactor);
    }

    public List<ClusterNode> replicasFor(Token token, int replicationFactor) {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replication factor must be positive");
        }
        if (replicationFactor > distinctNodeCount()) {
            throw new IllegalArgumentException("replication factor must not exceed distinct nodes");
        }

        int startIndex = locateStartIndex(token);
        ArrayList<ClusterNode> replicas = new ArrayList<>(replicationFactor);
        Set<NodeId> seenOwners = new LinkedHashSet<>(replicationFactor);
        int ringSize = vnodes.size();

        for (int step = 0; step < ringSize && replicas.size() < replicationFactor; step++) {
            Vnode vnode = vnodes.get((startIndex + step) % ringSize);
            if (seenOwners.add(vnode.owner().nodeId())) {
                replicas.add(vnode.owner());
            }
        }

        if (replicas.size() < replicationFactor) {
            throw new IllegalStateException("unable to place replicas across enough distinct nodes");
        }
        return List.copyOf(replicas);
    }

    public int distinctNodeCount() {
        return nodes.size();
    }

    public PartitionRingSnapshot withEpoch(long epoch) {
        return new PartitionRingSnapshot(epoch, vnodeCountPerNode, nodes, vnodes);
    }

    private int locateStartIndex(Token token) {
        int low = 0;
        int high = vnodes.size() - 1;
        int candidate = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Vnode vnode = vnodes.get(mid);
            int comparison = vnode.token().compareTo(token);
            if (comparison < 0) {
                low = mid + 1;
            } else {
                candidate = mid;
                high = mid - 1;
            }
        }

        return candidate >= 0 ? candidate : 0;
    }
}
