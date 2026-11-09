package com.wallet.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Starts the notification listener container on its own thread and retries until it starts, so a broker that is
 * down or unresolvable at start-up never aborts the application. Only exists when the consumer is enabled.
 */
@Component
@ConditionalOnProperty(name = "app.notifications.consumer.enabled", havingValue = "true")
public class NotificationConsumerLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumerLifecycle.class);

    private final KafkaListenerEndpointRegistry registry;
    private final Duration backoffMin;
    private final Duration backoffMax;
    private volatile boolean running;
    private volatile Thread thread;

    public NotificationConsumerLifecycle(KafkaListenerEndpointRegistry registry,
                                         @Value("${app.notifications.consumer.start-backoff-min:1s}") Duration backoffMin,
                                         @Value("${app.notifications.consumer.start-backoff-max:30s}") Duration backoffMax) {
        this.registry = registry;
        this.backoffMin = backoffMin;
        this.backoffMax = backoffMax;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(() -> {
            Duration wait = backoffMin;
            while (running) {
                try {
                    MessageListenerContainer c = registry.getListenerContainer(NotificationListener.LISTENER_ID);
                    if (c == null) {
                        throw new IllegalStateException("listener container not registered");
                    }
                    if (!c.isRunning()) {
                        c.start();
                    }
                    log.info("notification consumer started");
                    return;
                } catch (RuntimeException e) {
                    resetContainer(); // a failed start can leave the container marked as running: reset it before retrying
                    log.warn("notification consumer not started yet ({}); retrying in {} ms", e.getClass().getSimpleName(), wait.toMillis());
                }
                try {
                    Thread.sleep(wait.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                wait = wait.multipliedBy(2).compareTo(backoffMax) > 0 ? backoffMax : wait.multipliedBy(2);
            }
        }, "notification-consumer-starter");
        thread.setDaemon(true);
        thread.start();
    }

    private void resetContainer() {
        try {
            MessageListenerContainer c = registry.getListenerContainer(NotificationListener.LISTENER_ID);
            if (c != null) {
                c.stop();
            }
        } catch (RuntimeException ignored) {
            // best effort: the next attempt will report its own failure
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
        MessageListenerContainer c = registry.getListenerContainer(NotificationListener.LISTENER_ID);
        if (c != null && c.isRunning()) {
            c.stop();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
