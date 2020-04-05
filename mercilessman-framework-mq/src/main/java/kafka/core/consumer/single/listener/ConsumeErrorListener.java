package kafka.core.consumer.single.listener;


import kafka.core.HLClientConfig;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.single.ConsumeStatus;
import kafka.core.consumer.single.event.ConsumeErrorEvent;
import kafka.core.consumer.single.event.ReproduceEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ConsumeBizException;
import kafka.core.exception.ConsumeSysException;

public class ConsumeErrorListener
        extends AbstractListener<ConsumeErrorEvent>
        implements BroadcasterAware {
    private EventBroadcaster broadcaster;
    private final HLClientConfig config;

    public ConsumeErrorListener(HLClientConfig config) {
        addListeningEvent(ConsumeErrorEvent.class);

        this.config = config;
    }

    public void process(ConsumeErrorEvent event) {
        try {
            ConsumeContext consumeContext = event.getContext();
            ConsumeStatus errorStatus = consumeContext.getLastStatus();
            consumeContext.setLastStatus(ConsumeStatus.ERROR_HANDLING);
            consumeContext.setErrorStatus(errorStatus);

            Exception exception = event.getException();
            if ((exception instanceof ConsumeBizException)) {
                consumeContext.setException(exception);
            } else {
                consumeContext.setException(new ConsumeSysException(exception));
            }
            this.broadcaster.publish(new ReproduceEvent(consumeContext, consumeContext.getException()));
        } catch (Exception e) {
            this.broadcaster.publish(new ReproduceEvent(event.getContext(), event.getException()));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

