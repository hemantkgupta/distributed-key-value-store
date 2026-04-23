package com.hkg.kv.node;

import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.repair.FileHintStore;
import com.hkg.kv.repair.HintStore;
import com.hkg.kv.repair.HintedHandoffPlanner;
import com.hkg.kv.repair.HintedHandoffService;
import com.hkg.kv.repair.MerkleRangeStreamer;
import com.hkg.kv.repair.MerkleRepairLeaseStore;
import com.hkg.kv.replication.ReplicaReader;
import com.hkg.kv.replication.ReplicaWriter;
import com.hkg.kv.storage.StorageEngine;
import com.hkg.kv.storage.rocksdb.RocksDbStorageEngine;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KvNodeRuntime implements AutoCloseable {
    private final KvNodeConfig config;
    private final StorageEngine storage;
    private final MerkleRepairLeaseStore repairLeaseStore;
    private final HintStore hintStore;
    private final HttpReplicaTransportClient transportClient;
    private final CoordinatorService coordinatorService;
    private final HttpServer server;
    private final ExecutorService requestExecutor;
    private final ClusterNode localNode;
    private final AtomicBoolean closed;

    private KvNodeRuntime(
            KvNodeConfig config,
            StorageEngine storage,
            MerkleRepairLeaseStore repairLeaseStore,
            HintStore hintStore,
            HttpReplicaTransportClient transportClient,
            CoordinatorService coordinatorService,
            HttpServer server,
            ExecutorService requestExecutor,
            ClusterNode localNode
    ) {
        this.config = config;
        this.storage = storage;
        this.repairLeaseStore = repairLeaseStore;
        this.hintStore = hintStore;
        this.transportClient = transportClient;
        this.coordinatorService = coordinatorService;
        this.server = server;
        this.requestExecutor = requestExecutor;
        this.localNode = localNode;
        this.closed = new AtomicBoolean(false);
    }

    public static KvNodeRuntime start(KvNodeConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("node config must not be null");
        }

        StorageEngine storage = null;
        HttpServer server = null;
        ExecutorService requestExecutor = null;
        try {
            storage = RocksDbStorageEngine.open(config.storagePath());
            MerkleRepairLeaseStore repairLeaseStore = new RepairLeaseStoreFactory()
                    .create(config.repairLeaseStoreConfig());
            server = HttpServer.create(new InetSocketAddress(config.host(), config.port()), 0);
            requestExecutor = Executors.newCachedThreadPool(threadFactory(config));
            server.setExecutor(requestExecutor);

            HttpReplicaTransportHandlers handlers = new HttpReplicaTransportHandlers(config.nodeId(), storage);
            server.createContext(HttpReplicaTransportPaths.REPLICA_WRITE_PATH, handlers.replicaWriteHandler());
            server.createContext(HttpReplicaTransportPaths.REPLICA_READ_PATH, handlers.replicaReadHandler());
            server.createContext(
                    HttpReplicaTransportPaths.MERKLE_RANGE_STREAM_PATH,
                    handlers.merkleRangeStreamHandler()
            );
            server.start();

            ClusterNode localNode = new ClusterNode(
                    config.nodeId(),
                    config.host(),
                    server.getAddress().getPort(),
                    Map.of()
            );
            HintStore hintStore = new FileHintStore(config.coordinatorConfig().resolveHintStorePath(config.storagePath()));
            HttpReplicaTransportClient transportClient = new HttpReplicaTransportClient(
                    HttpClient.newHttpClient(),
                    config.requestTimeout()
            );
            CoordinatorService coordinatorService = new CoordinatorService(
                    config.coordinatorConfig().createReplicaPlanner(localNode),
                    config.coordinatorConfig(),
                    transportClient,
                    transportClient,
                    new HintedHandoffPlanner(new HintedHandoffService(hintStore))
            );
            HttpCoordinatorHandlers coordinatorHandlers = new HttpCoordinatorHandlers(coordinatorService);
            server.createContext(HttpCoordinatorPaths.COORDINATOR_WRITE_PATH, coordinatorHandlers.coordinatorWriteHandler());
            server.createContext(HttpCoordinatorPaths.COORDINATOR_READ_PATH, coordinatorHandlers.coordinatorReadHandler());
            return new KvNodeRuntime(
                    config,
                    storage,
                    repairLeaseStore,
                    hintStore,
                    transportClient,
                    coordinatorService,
                    server,
                    requestExecutor,
                    localNode
            );
        } catch (IOException | RuntimeException exception) {
            if (server != null) {
                server.stop(0);
            }
            if (requestExecutor != null) {
                requestExecutor.shutdownNow();
            }
            if (storage != null) {
                storage.close();
            }
            throw new IllegalStateException("failed to start KV node runtime", exception);
        }
    }

    public KvNodeConfig config() {
        return config;
    }

    public ClusterNode localNode() {
        return localNode;
    }

    public URI baseUri() {
        String host = localNode.host().contains(":") ? "[" + localNode.host() + "]" : localNode.host();
        return URI.create("http://" + host + ":" + localNode.port());
    }

    public StorageEngine storage() {
        return storage;
    }

    public MerkleRepairLeaseStore repairLeaseStore() {
        return repairLeaseStore;
    }

    public HintStore hintStore() {
        return hintStore;
    }

    public HttpReplicaTransportClient transportClient() {
        return transportClient;
    }

    public CoordinatorService coordinatorService() {
        return coordinatorService;
    }

    public ReplicaWriter replicaWriter() {
        return transportClient;
    }

    public ReplicaReader replicaReader() {
        return transportClient;
    }

    public MerkleRangeStreamer merkleRangeStreamer() {
        return transportClient;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        server.stop(0);
        requestExecutor.shutdownNow();
        storage.close();
    }

    private static ThreadFactory threadFactory(KvNodeConfig config) {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            thread.setName("kv-node-" + config.nodeId().value() + "-http");
            return thread;
        };
    }
}
