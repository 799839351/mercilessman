package kafka.core.producer.listener;


import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.component.Storage;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceStatus;
import kafka.core.producer.event.InitCompensateEvent;
import kafka.core.producer.event.SendingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CompensateListener extends AbstractListener<InitCompensateEvent> implements BroadcasterAware {

    public static final Logger LOGGER = LoggerFactory.getLogger(CompensateListener.class);
    private final HLClientConfig config;
    private final Storage storage;
    private EventBroadcaster broadcaster;

    public CompensateListener(HLClientConfig config, Storage storage) {
        addListeningEvent(InitCompensateEvent.class);

        this.config = config;
        this.storage = storage;
    }

    public void process(InitCompensateEvent event) {
        try {
            LOGGER.debug("[HL_MESSAGE] Initiating compensate process...");
            Map<String, Message<?>> messages = this.storage.load(event.getBatchSize());
            if (!messages.isEmpty()) {
                LOGGER.warn("[HL_MESSAGE] Compensation loaded " + messages.size() + " messages to resend.");
            }
            for (Map.Entry<String, Message<?>> entry : messages.entrySet()) {
                ProduceContext context = new ProduceContext((Message) entry.getValue());
                context.setContextId((String) entry.getKey());
                context.setLastStatus(ProduceStatus.COMPENSATE);
                context.setPersistent(true);
                this.broadcaster.publish(new SendingEvent(context), true);
            }
        } catch (Exception e) {
            LOGGER.error("[HL_MESSAGE] Error happened in compensate process.", e);
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

