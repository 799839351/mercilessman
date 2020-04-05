package kafka.core.consumer.batch;


import kafka.core.consumer.batch.event.PrepareBatchEvent;
import kafka.core.engine.EventBroadcaster;
import kafka.core.engine.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeInitiator implements Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumeInitiator.class);
    private final ScheduledExecutorService executorService;
    private final EventBroadcaster broadcaster;
    private volatile ScheduledFuture<?> future;
    private final AtomicBoolean busy;

    public ConsumeInitiator(EventBroadcaster broadcaster, AtomicBoolean busy) {
        this.broadcaster = broadcaster;
        this.busy = busy;

        this.executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("consume-init-thread"));
    }

    public boolean startConsume(long period, TimeUnit timeUnit) {
        if (this.future == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[HL_MESSAGE] Consume scheduled task submitted...");
            }
            Runnable initBatchTask = new Runnable() {
                public void run() {
                    try {
                        if (!ConsumeInitiator.this.busy.get()) {
                            ConsumeInitiator.this.broadcaster.publish(new PrepareBatchEvent());
                        }
                    } catch (Exception e) {
                        ConsumeInitiator.LOGGER.error("Fail to initiate new batch.", e);
                    }
                }
            };
            this.future = this.executorService.scheduleAtFixedRate(initBatchTask, 0L, period, timeUnit);
            return true;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[HL_MESSAGE] Consume scheduled task not submitted, because there is already a task running.");
        }
        return false;
    }

    public boolean stopConsume() {
        if (this.future == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[HL_MESSAGE] Stopping consume task failed, because no task is running.");
            }
            return false;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[HL_MESSAGE] Consume stopped.");
        }
        return this.future.cancel(true);
    }

    public void close() throws IOException {
        stopConsume();
        this.executorService.shutdown();
    }
}

