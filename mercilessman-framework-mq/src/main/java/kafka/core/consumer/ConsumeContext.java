package kafka.core.consumer;


import kafka.core.Message;
import kafka.core.MessageContext;
import kafka.core.consumer.single.ConsumeCallback;
import kafka.core.consumer.single.ConsumeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class ConsumeContext extends MessageContext {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumeContext.class);
    private final String contextId;
    private volatile ConsumeStatus lastStatus;
    private volatile ConsumeResult result;
    private volatile ConsumeCallback callback;
    private volatile Exception exception;
    private volatile ConsumeStatus errorStatus;
    private volatile Map<String, Object> consumeMetadata;
    private volatile long serverTimestamp;

    public ConsumeContext(Message<?> message) {
        this(message, null);
    }

    public ConsumeContext(Message<?> message, Map<String, Object> context) {
        super(message, context);
        this.lastStatus = ConsumeStatus.INIT;
        this.contextId = UUID.randomUUID().toString();
    }

    public ConsumeStatus getLastStatus() {
        return this.lastStatus;
    }

    public void setLastStatus(ConsumeStatus lastStatus) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[HL_MESSAGE] " + this + " is shifting status: [" + getLastStatus() + " --> " + lastStatus + "]");
        }
        this.lastStatus = lastStatus;
    }

    public ConsumeResult getResult() {
        return this.result;
    }

    public void setResult(ConsumeResult result) {
        this.result = result;
    }

    public ConsumeCallback getCallback() {
        return this.callback;
    }

    public void setCallback(ConsumeCallback callback) {
        this.callback = callback;
    }

    public Exception getException() {
        return this.exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public ConsumeStatus getErrorStatus() {
        return this.errorStatus;
    }

    public void setErrorStatus(ConsumeStatus errorStatus) {
        this.errorStatus = errorStatus;
    }

    public String getContextId() {
        return this.contextId;
    }

    public Map<String, Object> getConsumeMetadata() {
        return this.consumeMetadata;
    }

    public void setConsumeMetadata(Map<String, Object> consumeMetadata) {
        this.consumeMetadata = consumeMetadata;
    }

    public long getServerTimestamp() {
        return this.serverTimestamp;
    }

    public void setServerTimestamp(long serverTimestamp) {
        this.serverTimestamp = serverTimestamp;
    }

    public String toString() {
        return "ConsumeContext{contextId='" + this.contextId + '\'' + ", lastStatus=" + this.lastStatus + ", result=" + this.result + "} " + super.toString();
    }
}

