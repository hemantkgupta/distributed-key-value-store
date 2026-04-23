package com.hkg.kv.node;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class KvNodeMain {
    private KvNodeMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: KvNodeMain <properties-file>");
        }

        KvNodeConfig config = KvNodeConfig.fromProperties(load(Path.of(args[0])));
        KvNodeRuntime runtime = KvNodeRuntime.start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(runtime::close, "kv-node-shutdown"));
        System.out.println("KV node " + runtime.localNode().nodeId().value() + " listening at " + runtime.baseUri());

        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            runtime.close();
        }
    }

    private static Properties load(Path propertiesFile) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(propertiesFile)) {
            properties.load(inputStream);
        }
        return properties;
    }
}
