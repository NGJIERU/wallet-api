package com.wallet.ratelimit;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * The limiter HTTP context's OWN Redis (not the shared one). It binds a FIXED host port chosen at start, so a
 * stopped and restarted container comes back on the same port and the already-built context can reconnect.
 * No persistence. pause/unpause/stop/start are used by the failure tests; ensureRunning restores any state.
 */
final class LimiterRedis {

    /** addFixedExposedPort is protected, hence the small subclass. */
    private static final class FixedPortRedis extends GenericContainer<FixedPortRedis> {
        FixedPortRedis(int hostPort) {
            super("redis:7.2.17-alpine");
            addFixedExposedPort(hostPort, 6379);
        }
    }

    private static final int HOST_PORT = freePort();

    static final GenericContainer<?> REDIS = new FixedPortRedis(HOST_PORT)
            .withCommand("redis-server", "--appendonly", "no", "--save", "")
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));

    static {
        REDIS.start();
    }

    private LimiterRedis() {
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static String host() {
        return REDIS.getHost();
    }

    static int port() {
        return HOST_PORT;
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

    static void start() {
        REDIS.getDockerClient().startContainerCmd(REDIS.getContainerId()).exec();
    }

    /** Whatever state a test left it in: unpause if paused, start if stopped. */
    static void ensureRunning() {
        InspectContainerResponse.ContainerState state =
                REDIS.getDockerClient().inspectContainerCmd(REDIS.getContainerId()).exec().getState();
        if (Boolean.TRUE.equals(state.getPaused())) {
            unpause();
        } else if (!Boolean.TRUE.equals(state.getRunning())) {
            start();
        }
    }
}
