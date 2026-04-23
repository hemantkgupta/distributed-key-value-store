package com.hkg.kv.node;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.repair.HintDelivery;
import com.hkg.kv.repair.HintRecord;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicaWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class RuntimeHintDelivery implements HintDelivery {
    private final Map<NodeId, ClusterNode> clusterNodesByNodeId;
    private final ReplicaWriter replicaWriter;

    RuntimeHintDelivery(List<ClusterNode> clusterNodes, ReplicaWriter replicaWriter) {
        if (clusterNodes == null || clusterNodes.isEmpty()) {
            throw new IllegalArgumentException("cluster nodes must not be empty");
        }
        if (replicaWriter == null) {
            throw new IllegalArgumentException("replica writer must not be null");
        }
        LinkedHashMap<NodeId, ClusterNode> nodesById = new LinkedHashMap<>();
        for (ClusterNode clusterNode : clusterNodes) {
            if (clusterNode == null) {
                throw new IllegalArgumentException("cluster nodes must not contain null entries");
            }
            nodesById.put(clusterNode.nodeId(), clusterNode);
        }
        this.clusterNodesByNodeId = Map.copyOf(nodesById);
        this.replicaWriter = replicaWriter;
    }

    @Override
    public boolean deliver(HintRecord hint) {
        if (hint == null) {
            throw new IllegalArgumentException("hint must not be null");
        }
        ClusterNode target = clusterNodesByNodeId.get(hint.targetNodeId());
        if (target == null) {
            return false;
        }
        ReplicaResponse response = replicaWriter.write(target, hint.mutation());
        return response != null && response.success() && hint.targetNodeId().equals(response.nodeId());
    }
}
