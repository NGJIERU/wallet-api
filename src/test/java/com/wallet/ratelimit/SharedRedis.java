package com.wallet.ratelimit;

import org.testcontainers.containers.GenericContainer;

/** One Redis container for the whole test run, like the Postgres and Kafka holders. No persistence. */
final class SharedRedis {

    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.2.17-alpine")
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "no", "--save", "");

    static {
        REDIS.start();
    }

    private SharedRedis() {
    }

    static String host() {
        return REDIS.getHost();
    }

    static int port() {
        return REDIS.getMappedPort(6379);
    }
}
