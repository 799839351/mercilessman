package kafka.core.consumer.single.listener;


import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.consumer.Consumable;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.ConsumeResult;
import kafka.core.consumer.single.ConsumeStatus;
import kafka.core.consumer.single.event.ConsumeErrorEvent;
import kafka.core.consumer.single.event.DispatchEvent;
import kafka.core.consumer.single.event.PostProcessEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ConsumeBizException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DispatchListener extends AbstractListener<DispatchEvent>
        implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(DispatchListener.class);
    private EventBroadcaster broadcaster;
    private final HLClientConfig config;
    private final Map<String, Consumable<?>> consumableMap;

    public DispatchListener(HLClientConfig config) {
        addListeningEvent(DispatchEvent.class);

        this.config = config;

        this.consumableMap = ((Map) this.config.get("consumer.topic.relation"));
    }

    public void process(DispatchEvent event) {
        try {
            ConsumeContext context = event.getContext();
            context.setLastStatus(ConsumeStatus.DISPATCH);

            Message message = context.getMessage();
            ConsumeResult result;
            Consumable<?> consumable = (Consumable) this.consumableMap.get(message.topic());
            if (null == consumable) {
                throw new IllegalStateException("[HL_MESSAGE] No consumer found for topic: " + message.topic());
            }
            try {
                result = consumable.consume(message);
                if (null == result) {
                    throw new NullPointerException("[HL_MESSAGE] User returned consume result is null.");
                }
            } catch (Exception e) {
                LOGGER.error("Consumer threw an uncaught exception.", e);
                throw new ConsumeBizException(e);
            }

            context.setResult(result);
            this.broadcaster.publish(new PostProcessEvent(context));
        } catch (Exception e) {
            this.broadcaster.publish(new ConsumeErrorEvent(event.getContext(), e));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

