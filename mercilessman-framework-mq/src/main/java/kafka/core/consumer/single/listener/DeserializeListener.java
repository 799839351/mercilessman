package kafka.core.consumer.single.listener;


import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.component.ValueDeserializer;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.single.ConsumeStatus;
import kafka.core.consumer.single.event.ConsumeErrorEvent;
import kafka.core.consumer.single.event.DeserializeEvent;
import kafka.core.consumer.single.event.DispatchEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.util.ConfigUtils;

public class DeserializeListener extends AbstractListener<DeserializeEvent>
        implements BroadcasterAware {
    private final HLClientConfig config;
    private EventBroadcaster broadcaster;
    private final ValueDeserializer deserializer;

    public DeserializeListener(HLClientConfig config)
            throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        addListeningEvent(DeserializeEvent.class);

        this.config = config;

        Object deserializerConfig = this.config.get("consumer.value.deserializer");
        this.deserializer = ((ValueDeserializer) ConfigUtils.getInstance(ValueDeserializer.class, deserializerConfig));
        if (this.deserializer == null) {
            throw new IllegalStateException("[HL_MESSAGE] Value deserializer can't be initiated...");
        }
    }

    public void process(DeserializeEvent event) {
        try {
            ConsumeContext consumeContext = event.getContext();
            consumeContext.setLastStatus(ConsumeStatus.DESERIALIZE);

            Message<?> message = consumeContext.getMessage();
            Object value = message.value();
            if ((value instanceof String)) {
                value = this.deserializer.deserialize(message.topic(), (String) value);
            }
            Message<?> deserialized = new Message(message.topic(), message.key(), value, message.headers());
            consumeContext.setMessage(deserialized);

            this.broadcaster.publish(new DispatchEvent(consumeContext));
        } catch (Exception e) {
            this.broadcaster.publish(new ConsumeErrorEvent(event.getContext(), e));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

