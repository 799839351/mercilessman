package kafka.core.consumer.single.listener;


import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.single.ConsumeStatus;
import kafka.core.consumer.single.event.ConsumeFinishEvent;
import kafka.core.consumer.single.event.ReproduceEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ConsumeBizException;
import kafka.core.header.DelayUtil;
import kafka.core.header.ExceptionUtil;
import kafka.core.header.ReproduceUtil;
import kafka.core.producer.ProduceFuture;
import kafka.core.producer.ProduceResult;
import kafka.core.producer.Producer;
import kafka.core.util.ConfigUtils;
import kafka.core.util.DelayParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReproduceListener    extends AbstractListener<ReproduceEvent>
        implements BroadcasterAware
{
    public static final Logger LOGGER = LoggerFactory.getLogger(ReproduceListener.class);
    private final HLClientConfig config;
    private final Producer producer;
    private EventBroadcaster broadcaster;
    private final String consumerGroup;
    private final boolean enableVerboseLog;
    private final boolean enableMessageLog;
    private final boolean enableDLQ;
    private final boolean enableRetry;
    private final Map<String, List<Long>> retryDelays;
    private final List<Long> defaultDelay;

    public ReproduceListener(HLClientConfig config, Producer producer)
    {
        addListeningEvent(ReproduceEvent.class);

        this.config = config;
        this.producer = producer;

        this.consumerGroup = ConfigUtils.getString(config.get("consumer.group"));
        this.enableVerboseLog = ConfigUtils.getBoolean(config.get("client.log.verbose")).booleanValue();
        this.enableMessageLog = ConfigUtils.getBoolean(config.get("client.log.show.msg")).booleanValue();
        this.enableRetry = ConfigUtils.getBoolean(config.get("consumer.enable.retry")).booleanValue();
        this.enableDLQ = ConfigUtils.getBoolean(config.get("consumer.enable.dlq")).booleanValue();
        this.retryDelays = parseRetryDelays((Map)config.get("consumer.retry.delay.sec"));

        this.defaultDelay = DelayParseUtil.parseDelay(ConfigUtils.getString(config.get("consumer.retry.delay.sec.default")));
    }

    public static Map<String, List<Long>> parseRetryDelays(Map<String, String> delayConfig)
    {
        Map<String, List<Long>> result = new HashMap(8);
        for (Map.Entry<String, String> delay : delayConfig.entrySet())
        {
            String topic = (String)delay.getKey();
            List<Long> delays = DelayParseUtil.parseDelay((String)delay.getValue());
            result.put(topic, delays);
        }
        return result;
    }

    public void process(ReproduceEvent event)
    {
        try
        {
            ConsumeContext context = event.getContext();
            context.setLastStatus(ConsumeStatus.REPRODUCE);

            Message<?> message = event.getContext().getMessage();
            ExceptionUtil.putConsumeException(message, event.getCause());

            String topic = message.topic();
            List<Long> delay = this.retryDelays.containsKey(topic) ? (List)this.retryDelays.get(topic) : this.defaultDelay;
            int totalFailTimes;
            if ((event.getCause() instanceof ConsumeBizException)) {
                totalFailTimes = ReproduceUtil.logBizFailure(message);
            } else {
                totalFailTimes = ReproduceUtil.logSysFailure(message);
            }
            boolean reproduceResult = true;
            if (this.enableVerboseLog) {
                LOGGER.warn("[HL_MESSAGE] " + getMessageString(message) + " consume failed. This message has failed for [" + totalFailTimes + "] time(s).", event.getCause());
            }
            if (totalFailTimes > delay.size())
            {
                if (this.enableDLQ)
                {
                    reproduceResult = reproduceToDLQ(message);
                    LOGGER.warn("[HL_MESSAGE] " + getMessageString(message) + " has failed all its " + delay.size() + " retry chance(s), and is being sent to DLQ.", event.getCause());
                }
                else
                {
                    LOGGER.warn("[HL_MESSAGE] " + getMessageString(message) + " has failed all its [" + delay.size() + "] retry chance(s), and is now IGNORED because DLQ is disabled.", event.getCause());
                }
            }
            else if (this.enableRetry)
            {
                Long delaySec = (Long)delay.get(totalFailTimes - 1);
                reproduceResult = reproduceToRetryQueue(message, delaySec.longValue());
                if (this.enableVerboseLog) {
                    LOGGER.warn("[HL_MESSAGE] " + getMessageString(message) + " will be retried after [" + delaySec + "] seconds.");
                }
            }
            else if (this.enableVerboseLog)
            {
                LOGGER.warn("[HL_MESSAGE] " + getMessageString(message) + " is now IGNORED because retry is disabled.");
            }
            if (reproduceResult) {
                this.broadcaster.publish(new ConsumeFinishEvent(context, true));
            } else {
                this.broadcaster.publish(new ConsumeFinishEvent(event.getContext(), false));
            }
        }
        catch (Exception e)
        {
            LOGGER.error("[HL_MESSAGE] Error happened in reproduce process.", e);
            this.broadcaster.publish(new ConsumeFinishEvent(event.getContext(), false));
        }
    }

    private String getMessageString(Message<?> message)
    {
        if (this.enableMessageLog) {
            return message.toString();
        }
        return "Message@" + hashCode();
    }

    private boolean reproduceToDLQ(Message<?> message)
            throws ExecutionException, InterruptedException
    {
        ReproduceUtil.setDlqFlag(message, true);
        ReproduceUtil.setConsumedBy(message, this.consumerGroup);
        ReproduceUtil.resetBizFailure(message);
        ReproduceUtil.resetSysFailure(message);
        ProduceFuture future = this.producer.send(message);
        ProduceResult result = future.get();
        return result.isSuccess();
    }

    private boolean reproduceToRetryQueue(Message<?> message, long delaySec)
            throws ExecutionException, InterruptedException
    {
        ReproduceUtil.removeDlqFlag(message);
        ReproduceUtil.setRetryFlag(message, true);
        ReproduceUtil.setConsumedBy(message, this.consumerGroup);
        DelayUtil.setConsumeDelay(message, delaySec);
        ProduceFuture future = this.producer.send(message);
        ProduceResult result = future.get();
        return result.isSuccess();
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster)
    {
        this.broadcaster = eventBroadcaster;
    }
}
