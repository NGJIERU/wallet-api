package com.wallet.ratelimit;

import org.testcontainers.containers.GenericContainer;

/**
 * The limiter HTTP context's OWN Redis (not the shared one), so 6.3 can pause or stop it without touching
 * other classes. No persistence. pause/unpause/stop are unused until 6.3.
 */
final class LimiterRedis {

    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.2.17-alpine")
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "no", "--save", "");

    static {
        REDIS.start();
    }

    private LimiterRedis() {
    }

    static String host() {
        return REDIS.getHost();
    }

    static int port() {
        return REDIS.getMappedPort(6379);
    }

    static void pause() {
        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
    }

    static void unpause() {
        REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
    }

    static void stop() {
        REDIS.getDockerClient().stopContainerCmd(REDIS.getContainerId()).exec();
    }
}
