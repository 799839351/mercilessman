package kafka.core.producer.listener;


import kafka.core.HLClientConfig;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ProduceException;
import kafka.core.producer.ProduceCallback;
import kafka.core.producer.ProduceContext;
import kafka.core.producer.ProduceStatus;
import kafka.core.producer.event.ProduceFinishEvent;
import kafka.core.producer.event.SendingErrorEvent;
import kafka.core.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendingErrorListener extends AbstractListener<SendingErrorEvent>
        implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(SendingErrorListener.class);
    private EventBroadcaster broadcaster;
    private final HLClientConfig config;
    private boolean enableLogMessage;

    public SendingErrorListener(HLClientConfig config) {
        addListeningEvent(SendingErrorEvent.class);
        this.config = config;
        this.enableLogMessage = ConfigUtils.getBoolean(config.get("client.log.show.msg")).booleanValue();
    }

    public void process(SendingErrorEvent event) {
        try {
            ProduceContext context = event.getContext();
            ProduceStatus errorStatus = context.getLastStatus();
            context.setLastStatus(ProduceStatus.ERROR_HANDLING);

            context.setProduceException(new ProduceException("Error happened in " + errorStatus + " process.", event.getException()));

            context.setErrorStatus(errorStatus);

            StringBuilder builder = new StringBuilder("Error happened in " + errorStatus + " status.\n");
            if (this.enableLogMessage) {
                builder.append("Message: ").append(context.getMessage()).append("\n");
            }
            if (errorStatus.compareTo(ProduceStatus.LOCAL_STORE) <= 0) {
                builder.append("This message is not yet persistent, and will be LOST if you ignore this exception.");
            } else if (errorStatus.compareTo(ProduceStatus.SEND) <= 0) {
                builder.append("This message is now persistent if you enabled compensation, and you are safe to continue, we will retry later.");
            } else if (errorStatus.compareTo(ProduceStatus.SEND) > 0) {
                builder.append("This message is now sent but the post-process failed.");
            }
            LOGGER.error("[HL_MESSAGE] " + builder.toString(), event.getException());

            ProduceCallback callback = context.getProduceCallback();
            if (null != callback) {
                if (context.judgeSuccess()) {
                    callback.afterSendSuccess(context);
                } else {
                    callback.afterSendFailure(context, event.getException());
                }
            }
            this.broadcaster.publish(new ProduceFinishEvent(context));
        } catch (Exception e) {
            LOGGER.error("[HL_MESSAGE] Error happened during error processing...", e);
            this.broadcaster.publish(new ProduceFinishEvent(event.getContext()));
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}
