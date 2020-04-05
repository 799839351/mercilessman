package kafka.core.producer;

import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.MessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProduceContext extends MessageContext {
    public static final Logger LOGGER = LoggerFactory.getLogger(ProduceContext.class);
    private volatile String contextId;
    private volatile ProduceCallback produceCallback;
    private volatile Map<String, Object> sendMetadata;
    private volatile Exception produceException;
    private volatile ProduceStatus errorStatus;
    private volatile boolean isCanceled;
    private volatile boolean isSent;
    private volatile boolean isPersistent;
    private final CountDownLatch produceLatch;
    private ProduceStatus lastStatus;
    private ProduceStatus confirmStatus;
    private boolean isConfirmed;

    public ProduceContext(Message<?> message) {
        this(message, null);
    }

    public ProduceContext(Message<?> message, Map<String, Object> context) {
        super(message, context);
        this.contextId = UUID.randomUUID().toString();

        this.produceLatch = new CountDownLatch(1);
        this.lastStatus = ProduceStatus.INIT;
        this.confirmStatus = ProduceStatus.LOCAL_STORE;

        this.isConfirmed = false;
        this.isCanceled = false;
        this.isSent = false;
        this.isPersistent = false;
    }


    public ProduceCallback getProduceCallback()
    {
        return this.produceCallback;
    }

    public void setProduceCallback(ProduceCallback produceCallback)
    {
        this.produceCallback = produceCallback;
    }

    public Map<String, Object> getSendMetadata()
    {
        return this.sendMetadata;
    }

    public void setSendMetadata(Map<String, Object> sendMetadata)
    {
        this.sendMetadata = sendMetadata;
    }

    public String getContextId()
    {
        return this.contextId;
    }

    public Exception getProduceException()
    {
        return this.produceException;
    }

    public void setProduceException(Exception produceException)
    {
        this.produceException = produceException;
    }

    public ProduceStatus getErrorStatus()
    {
        return this.errorStatus;
    }

    public void setErrorStatus(ProduceStatus errorStatus)
    {
        this.errorStatus = errorStatus;
    }

    public synchronized ProduceStatus getLastStatus()
    {
        return this.lastStatus;
    }

    public synchronized void setLastStatus(ProduceStatus lastStatus)
    {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[HL_MESSAGE] " + this + " is shifting status: [" + getLastStatus() + " --> " + lastStatus + "]");
        }
        this.lastStatus = lastStatus;
        if ((this.lastStatus.compareTo(this.confirmStatus) > 0) || (this.lastStatus == ProduceStatus.END))
        {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[HL_MESSAGE] Context: " + this + " is done, and releasing produce latch.");
            }
            this.produceLatch.countDown();
            this.isConfirmed = true;
        }
    }

    public synchronized void setConfirmStatus(ProduceStatus confirmStatus)
    {
        this.confirmStatus = confirmStatus;
    }

    public void waitTillDone()
            throws InterruptedException
    {
        this.produceLatch.await();
    }

    public boolean waitTillDone(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return this.produceLatch.await(timeout, unit);
    }

    public synchronized boolean isCanceled()
    {
        return this.isCanceled;
    }

    public synchronized boolean cancel()
    {
        if (!this.isSent)
        {
            this.isCanceled = true;
            return true;
        }
        return false;
    }

    public synchronized void markSent()
    {
        this.isSent = true;
    }

    public synchronized boolean isConfirmed()
    {
        return this.isConfirmed;
    }

    public void setContextId(String contextId)
    {
        this.contextId = contextId;
    }

    public boolean judgeSuccess()
    {
        if (this.errorStatus == null) {
            return true;
        }
        return this.isPersistent;
    }

    public void setPersistent(boolean persistent)
    {
        this.isPersistent = persistent;
    }

    public boolean isPersistent()
    {
        return this.isPersistent;
    }

    public String toString()
    {
        return "ProduceContext{contextId='" + this.contextId + '\'' + ", produceException=" + this.produceException + ", lastStatus=" + this.lastStatus + "} " + super.toString();
    }
}
