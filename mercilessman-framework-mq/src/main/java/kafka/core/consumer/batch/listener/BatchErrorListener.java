package kafka.core.consumer.batch.listener;


import kafka.core.HLClientConfig;
import kafka.core.consumer.batch.BatchContext;
import kafka.core.consumer.batch.BatchStatus;
import kafka.core.consumer.batch.event.BatchErrorEvent;
import kafka.core.consumer.batch.event.BatchFinishEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ConsumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchErrorListener extends AbstractListener<BatchErrorEvent> implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(BatchErrorListener.class);
    private final HLClientConfig config;
    private EventBroadcaster broadcaster;

    public BatchErrorListener(HLClientConfig config) {
        addListeningEvent(BatchErrorEvent.class);

        this.config = config;
    }

    public void process(BatchErrorEvent event) {
        try {
            BatchContext context = event.getContext();
            BatchStatus status = event.getContext().getStatus();

            context.setStatus(BatchStatus.ERROR_HANDLING);
            Exception exception = event.getException();

            LOGGER.error("[HL_MESSAGE] Batch consuming encountered exception in stage: " + status, new ConsumeException(exception));

            this.broadcaster.publish(new BatchFinishEvent(event.getContext()));
        } catch (Exception e) {
            LOGGER.error("[HL_MESSAGE] Batch error handling failed, ending batch anyway.", e);
            this.broadcaster.publish(new BatchFinishEvent(event.getContext()));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

