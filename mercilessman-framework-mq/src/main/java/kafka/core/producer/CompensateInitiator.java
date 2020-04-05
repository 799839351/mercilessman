package kafka.core.producer;


import kafka.core.HLClientConfig;
import kafka.core.engine.EventBroadcaster;
import kafka.core.engine.NamedThreadFactory;
import kafka.core.producer.event.InitCompensateEvent;
import kafka.core.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompensateInitiator implements Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompensateInitiator.class);
    private final HLClientConfig config;
    private final EventBroadcaster broadcaster;
    private final ScheduledExecutorService executorService;
    private final int compensateIntervalMs;
    private final int compensateBatchSize;

    public CompensateInitiator(HLClientConfig config, EventBroadcaster broadcaster) {
        this.config = config;
        this.broadcaster = broadcaster;
        this.executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("compensate-init-thread"));

        this.compensateBatchSize = ConfigUtils.getInteger(config.get("producer.compensate.batch.size")).intValue();

        this.compensateIntervalMs = ConfigUtils.getInteger(config.get("producer.compensate.interval.ms")).intValue();
    }

    public void start() {
        Runnable compensateTask = new Runnable() {
            public void run() {
                try {
                    CompensateInitiator.this.broadcaster.publish(new InitCompensateEvent(CompensateInitiator.this.compensateBatchSize), true);
                } catch (Exception e) {
                    CompensateInitiator.LOGGER.error("Fail to submit compensate task...", e);
                }
            }
        };
        this.executorService.scheduleWithFixedDelay(compensateTask, 100L, this.compensateIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void close()
            throws IOException {
        this.executorService.shutdownNow();
    }
}

