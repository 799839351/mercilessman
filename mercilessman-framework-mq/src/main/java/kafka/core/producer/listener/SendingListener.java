package kafka.core.producer.listener;


import kafka.core.component.Sender;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceStatus;
import kafka.core.producer.SendCallback;
import kafka.core.producer.event.PostSendingEvent;
import kafka.core.producer.event.SendingErrorEvent;
import kafka.core.producer.event.SendingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SendingListener
        extends AbstractListener<SendingEvent>
        implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(SendingListener.class);
    private EventBroadcaster broadcaster;
    private Sender sender;

    public SendingListener(Sender sender) {
        addListeningEvent(SendingEvent.class);
        this.sender = sender;
    }

    public void process(SendingEvent event) {
        try {
            final ProduceContext context = event.getContext();
            context.setLastStatus(ProduceStatus.SEND);
            if (!context.isCanceled()) {
                context.markSent();
                this.sender.send(context, (metadata, e) -> {
                    context.setSendMetadata(metadata);
                    if (e == null) {
                        SendingListener.this.broadcaster.publish(new PostSendingEvent(context));
                        SendingListener.LOGGER.debug("Message: " + context + " is successfully sent.");
                    } else {
                        SendingListener.this.broadcaster.publish(new SendingErrorEvent(context, e));
                        SendingListener.LOGGER.debug("Message: " + context + " has encountered problem when sending.", e);
                    }
                });
            }
        } catch (Exception e) {
            ProduceContext context = event.getContext();
            this.broadcaster.publish(new SendingErrorEvent(context, e));
            LOGGER.debug("Message: " + context + " has encountered problem when sending.", e);
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

