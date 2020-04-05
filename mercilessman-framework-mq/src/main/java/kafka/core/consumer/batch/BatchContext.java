package kafka.core.consumer.batch;


import kafka.core.consumer.ConsumeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BatchContext
{
    public static final Logger LOGGER = LoggerFactory.getLogger(BatchContext.class);
    private final String contextId;
    private volatile BatchStatus status;
    private volatile LinkedList<ConsumeContext> messages;
    private Integer messageCount;
    private volatile CountDownLatch batchLatch;

    public BatchContext()
    {
        this.contextId = UUID.randomUUID().toString();
        this.status = BatchStatus.INIT;
        this.messages = new LinkedList();
        this.messageCount = Integer.valueOf(0);
    }

    public BatchStatus getStatus()
    {
        return this.status;
    }

    public void setStatus(BatchStatus status)
    {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[HL_MESSAGE] " + this + " is shifting status: [" + getStatus() + " --> " + status + "]");
        }
        this.status = status;
    }

    public Queue<ConsumeContext> getMessages()
    {
        return this.messages;
    }

    public void setMessages(Collection<ConsumeContext> messages)
    {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[HL_MESSAGE] " + this + " is loaded with " + messages.size() + " messages.");
        }
        this.messages = new LinkedList(messages);
        this.messageCount = Integer.valueOf(messages.size());
        this.batchLatch = new CountDownLatch(messages.size());
    }

    public String getContextId()
    {
        return this.contextId;
    }

    public String toString()
    {
        return "BatchContext{contextId='" + this.contextId + '\'' + ", status=" + this.status + '}';
    }

    public void recall(ConsumeContext consumeContext)
    {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[HL_MESSAGE] " + consumeContext + " is recalled.");
        }
        this.batchLatch.countDown();
    }

    public boolean waitForRecall(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return this.batchLatch.await(timeout, unit);
    }

    public long countPending()
    {
        return this.batchLatch.getCount();
    }

    public Integer getMessageCount()
    {
        return this.messageCount;
    }
}

