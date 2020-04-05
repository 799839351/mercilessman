package kafka.core.consumer.single.listener;


import kafka.core.HLClientConfig;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.ConsumeProcessor;
import kafka.core.consumer.ConsumeResult;
import kafka.core.consumer.single.ConsumeStatus;
import kafka.core.consumer.single.event.ConsumeErrorEvent;
import kafka.core.consumer.single.event.ConsumeFinishEvent;
import kafka.core.consumer.single.event.PostProcessEvent;
import kafka.core.consumer.single.event.ReproduceEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ConsumeBizException;
import kafka.core.util.ConfigUtils;
import org.apache.commons.lang3.StringUtils;

public class PostProcessListener extends AbstractListener<PostProcessEvent>
        implements BroadcasterAware
{
    private final ConsumeProcessor postProcessor;
    private final HLClientConfig config;
    private EventBroadcaster broadcaster;

    public PostProcessListener(HLClientConfig config)
            throws IllegalAccessException, ClassNotFoundException, InstantiationException
    {
        addListeningEvent(PostProcessEvent.class);

        this.config = config;

        Object consumeProcessor = config.get("consumer.post.processor");
        if (null != consumeProcessor) {
            this.postProcessor = ((ConsumeProcessor) ConfigUtils.getInstance(ConsumeProcessor.class, consumeProcessor));
        } else {
            this.postProcessor = null;
        }
    }

    public void process(PostProcessEvent event)
    {
        try
        {
            ConsumeContext consumeContext = event.getContext();
            consumeContext.setLastStatus(ConsumeStatus.POST_PROCESS);
            if (null != this.postProcessor) {
                this.postProcessor.process(consumeContext);
            }
            if (event.getContext().getResult().getSuccess())
            {
                this.broadcaster.publish(new ConsumeFinishEvent(consumeContext, true));
            }
            else
            {
                ConsumeResult result = event.getContext().getResult();
                String message = StringUtils.isEmpty(result.getMessage()) ? "Result confirm failed." : result.getMessage();

                this.broadcaster.publish(new ReproduceEvent(consumeContext, new ConsumeBizException(message)));
            }
        }
        catch (Exception e)
        {
            this.broadcaster.publish(new ConsumeErrorEvent(event.getContext(), e));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster)
    {
        this.broadcaster = eventBroadcaster;
    }
}

