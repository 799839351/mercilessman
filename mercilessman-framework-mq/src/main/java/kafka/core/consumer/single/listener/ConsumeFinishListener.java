package kafka.core.consumer.single.listener;

import kafka.core.HLClientConfig;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.single.ConsumeCallback;
import kafka.core.consumer.single.ConsumeStatus;
import kafka.core.consumer.single.event.ConsumeFinishEvent;
import kafka.core.engine.AbstractListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeFinishListener extends AbstractListener<ConsumeFinishEvent> {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumeFinishListener.class);
    private final HLClientConfig config;

    public ConsumeFinishListener(HLClientConfig config) {
        addListeningEvent(ConsumeFinishEvent.class);

        this.config = config;
    }

    public void process(ConsumeFinishEvent event) {
        try {
            ConsumeContext consumeContext = event.getContext();
            consumeContext.setLastStatus(ConsumeStatus.FINISH);

            ConsumeCallback callback = event.getContext().getCallback();
            callback.afterConsume(event.getCommitable());

            consumeContext.setLastStatus(ConsumeStatus.END);
        } catch (Exception e) {
            LOGGER.error("[HL_MESSAGE] Error happened in message recall process, try again.", e);
            event.getContext().getCallback().afterConsume(false);
        }
    }
}

