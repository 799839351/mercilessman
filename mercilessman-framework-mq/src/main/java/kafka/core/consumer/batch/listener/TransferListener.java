package kafka.core.consumer.batch.listener;


import kafka.core.HLClientConfig;
import kafka.core.component.Recorder;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.batch.BatchContext;
import kafka.core.consumer.batch.BatchStatus;
import kafka.core.consumer.batch.event.AwaitRecallEvent;
import kafka.core.consumer.batch.event.BatchErrorEvent;
import kafka.core.consumer.batch.event.TransferEvent;
import kafka.core.consumer.single.ConsumeCallback;
import kafka.core.consumer.single.SingleConsumer;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferListener
        extends AbstractListener<TransferEvent>
        implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransferListener.class);
    private final HLClientConfig config;
    private final SingleConsumer singleConsumer;
    private final Recorder recorder;
    private EventBroadcaster broadcaster;

    public TransferListener(HLClientConfig config, SingleConsumer singleConsumer, Recorder recorder) {
        addListeningEvent(TransferEvent.class);

        this.config = config;
        this.singleConsumer = singleConsumer;
        this.recorder = recorder;
    }

    public void process(TransferEvent event) {
        try {
            final BatchContext batchContext = event.getContext();
            batchContext.setStatus(BatchStatus.TRANSFER);
            if ((!batchContext.getMessages().isEmpty()) && (LOGGER.isDebugEnabled())) {
                LOGGER.debug("[HL_MESSAGE] " + batchContext + " transferring " + batchContext.getMessages().size() + " messages to singleConsumer.");
            }
            while (!batchContext.getMessages().isEmpty()) {
                final ConsumeContext consumeContext = (ConsumeContext) batchContext.getMessages().poll();
                if (null == consumeContext) {
                    break;
                }
                this.recorder.record(consumeContext);
                ConsumeCallback callback = new ConsumeCallback() {
                    public void afterConsume(boolean commitable) {
                        TransferListener.this.recorder.markCommitStatus(consumeContext, commitable);
                        batchContext.recall(consumeContext);
                    }
                };
                this.singleConsumer.offerMessage(consumeContext, callback);
            }
            this.broadcaster.publish(new AwaitRecallEvent(batchContext));
        } catch (Exception e) {
            this.broadcaster.publish(new BatchErrorEvent(event.getContext(), e));
        } finally {
            event.getContext().getMessages().clear();
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

