package kafka.core.consumer.batch.listener;


import kafka.core.HLClientConfig;
import kafka.core.consumer.batch.BatchContext;
import kafka.core.consumer.batch.BatchStatus;
import kafka.core.consumer.batch.event.PollEvent;
import kafka.core.consumer.batch.event.PrepareBatchEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class PrepareBatchListener
        extends AbstractListener<PrepareBatchEvent>
        implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(PrepareBatchListener.class);
    private EventBroadcaster broadcaster;
    private final HLClientConfig config;
    private final AtomicBoolean isBusy;
    private final AtomicBoolean isRunning;

    public PrepareBatchListener(HLClientConfig config, AtomicBoolean isBusy, AtomicBoolean isRunning) {
        addListeningEvent(PrepareBatchEvent.class);

        this.config = config;
        this.isBusy = isBusy;
        this.isRunning = isRunning;
    }

    public void process(PrepareBatchEvent event) {
        try {
            if (!this.isRunning.get()) {
                return;
            }
            if (this.isBusy.compareAndSet(false, true)) {
                BatchContext context = new BatchContext();
                LOGGER.debug("[HL_MESSAGE] BatchContext: " + context + " is initiated...");
                context.setStatus(BatchStatus.PREPARE);
                this.broadcaster.publish(new PollEvent(context));
            } else {
                LOGGER.debug("[HL_MESSAGE] Batch consumer is processing another batch, ignoring new poll task.");
            }
        } catch (Exception e) {
            LOGGER.error("[HL_MESSAGE] Prepare poll has failed, aborting this batch.", e);
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

