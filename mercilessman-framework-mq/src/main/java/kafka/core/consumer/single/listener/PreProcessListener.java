package kafka.core.consumer.single.listener;


import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.ConsumeProcessor;
import kafka.core.consumer.single.ConsumeStatus;
import kafka.core.consumer.single.event.ConsumeErrorEvent;
import kafka.core.consumer.single.event.ConsumeFinishEvent;
import kafka.core.consumer.single.event.DeserializeEvent;
import kafka.core.consumer.single.event.PreProcessEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.header.DelayUtil;
import kafka.core.util.ConfigUtils;

public class PreProcessListener
        extends AbstractListener<PreProcessEvent>
        implements BroadcasterAware
{
    private final ConsumeProcessor preProcessor;
    private final HLClientConfig config;
    private EventBroadcaster broadcaster;

    public PreProcessListener(HLClientConfig config)
            throws IllegalAccessException, ClassNotFoundException, InstantiationException
    {
        addListeningEvent(PreProcessEvent.class);

        this.config = config;

        Object consumeProcessor = config.get("consumer.pre.processor");
        if (null != consumeProcessor) {
            this.preProcessor = ((ConsumeProcessor) ConfigUtils.getInstance(ConsumeProcessor.class, consumeProcessor));
        } else {
            this.preProcessor = null;
        }
    }

    public void process(PreProcessEvent event)
    {
        try
        {
            ConsumeContext consumeContext = event.getContext();
            consumeContext.setLastStatus(ConsumeStatus.PRE_PROCESS);
            if (!isDue(consumeContext))
            {
                this.broadcaster.publish(new ConsumeFinishEvent(consumeContext, false));
                return;
            }
            if (null != this.preProcessor) {
                this.preProcessor.process(consumeContext);
            }
            this.broadcaster.publish(new DeserializeEvent(consumeContext));
        }
        catch (Exception e)
        {
            this.broadcaster.publish(new ConsumeErrorEvent(event.getContext(), e));
        }
    }

    private boolean isDue(ConsumeContext context)
    {
        Message<?> message = context.getMessage();
        if (!DelayUtil.hasConsumeDelay(message)) {
            return true;
        }
        long delaySecond = DelayUtil.getConsumeDelay(message);
        long serverTime = context.getServerTimestamp();
        long now = System.currentTimeMillis();
        if (serverTime + delaySecond * 1000L < now)
        {
            DelayUtil.removeConsumeDelay(message);
            return true;
        }
        return false;
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster)
    {
        this.broadcaster = eventBroadcaster;
    }
}

