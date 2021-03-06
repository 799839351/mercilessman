package kafka.core.producer.listener;


import kafka.core.HLClientConfig;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceProcessor;
import kafka.core.producer.ProduceStatus;
import kafka.core.producer.event.PostSendingEvent;
import kafka.core.producer.event.ProduceFinishEvent;
import kafka.core.producer.event.SendingErrorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class PostSendProcessListener extends AbstractListener<PostSendingEvent>
        implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(PostSendProcessListener.class);
    private final Map<ProduceStatus, ProduceProcessor> processors;
    private EventBroadcaster broadcaster;
    private final HLClientConfig config;

    public PostSendProcessListener(HLClientConfig config) {
        addListeningEvent(PostSendingEvent.class);
        this.processors = new ConcurrentSkipListMap();
        this.config = config;
    }

    public void addProcessor(ProduceStatus status, ProduceProcessor produceProcessor) {
        this.processors.put(status, produceProcessor);
    }

    public void removeProcessor(ProduceStatus status) {
        this.processors.remove(status);
    }

    public Map<ProduceStatus, ProduceProcessor> getProcessors() {
        return Collections.unmodifiableMap(this.processors);
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }

    public void process(PostSendingEvent event) {
        try {
            ProduceContext context = event.getContext();
            for (Map.Entry<ProduceStatus, ProduceProcessor> entry : this.processors.entrySet()) {
                context.setLastStatus((ProduceStatus) entry.getKey());
                ((ProduceProcessor) entry.getValue()).process(context);
            }
            this.broadcaster.publish(new ProduceFinishEvent(context));
        } catch (Exception e) {
            ProduceContext context = event.getContext();
            this.broadcaster.publish(new SendingErrorEvent(context, e));
        }
    }
}

