package kafka.core.consumer.batch.listener;


import kafka.core.HLClientConfig;
import kafka.core.component.Receiver;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.batch.BatchContext;
import kafka.core.consumer.batch.BatchStatus;
import kafka.core.consumer.batch.event.BatchErrorEvent;
import kafka.core.consumer.batch.event.PollEvent;
import kafka.core.consumer.batch.event.TransferEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.util.ConfigUtils;

import java.util.Collection;

public class PollListener
        extends AbstractListener<PollEvent>
        implements BroadcasterAware {
    private final HLClientConfig config;
    private final Receiver receiver;
    private EventBroadcaster broadcaster;
    private long pollTimeout;

    public PollListener(HLClientConfig config, Receiver receiver) {
        addListeningEvent(PollEvent.class);

        this.config = config;
        this.receiver = receiver;

        this.pollTimeout = ConfigUtils.getLong(config.get("consumer.poll.timeout.ms")).longValue();
    }

    public void process(PollEvent event) {
        try {
            BatchContext batchContext = event.getContext();
            batchContext.setStatus(BatchStatus.POLL);

            Collection<ConsumeContext> messages = this.receiver.poll(this.pollTimeout);
            batchContext.setMessages(messages);
            this.broadcaster.publish(new TransferEvent(batchContext));
        } catch (Exception e) {
            this.broadcaster.publish(new BatchErrorEvent(event.getContext(), e));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

