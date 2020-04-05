package kafka.core.consumer.batch.listener;


import kafka.core.HLClientConfig;
import kafka.core.consumer.batch.BatchContext;
import kafka.core.consumer.batch.BatchStatus;
import kafka.core.consumer.batch.event.AwaitRecallEvent;
import kafka.core.consumer.batch.event.BatchErrorEvent;
import kafka.core.consumer.batch.event.BatchFinishEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class AwaitRecallListener extends AbstractListener<AwaitRecallEvent> implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwaitRecallListener.class);
    private final HLClientConfig config;
    private EventBroadcaster broadcaster;
    private final long batchTimeoutSec;

    public AwaitRecallListener(HLClientConfig config) {
        addListeningEvent(AwaitRecallEvent.class);

        this.config = config;

        this.batchTimeoutSec = ConfigUtils.getLong(config.get("consumer.batch.timeout.sec")).longValue();
    }

    public void process(AwaitRecallEvent event) {
        try {
            BatchContext batchContext = event.getContext();
            batchContext.setStatus(BatchStatus.AWAIT);

            boolean allRecalled = batchContext.waitForRecall(this.batchTimeoutSec, TimeUnit.SECONDS);
            if (!allRecalled) {
                LOGGER.warn("[HL_MESSAGE] Batch: " + batchContext + " is timed out, " + batchContext.countPending() + " messages still pending.");
            }
            this.broadcaster.publish(new BatchFinishEvent(batchContext));
        } catch (Exception e) {
            this.broadcaster.publish(new BatchErrorEvent(event.getContext(), e));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}
