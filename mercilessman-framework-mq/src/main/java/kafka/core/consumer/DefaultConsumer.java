package kafka.core.consumer;


import kafka.core.HLClientConfig;
import kafka.core.component.Receiver;
import kafka.core.component.Recorder;
import kafka.core.consumer.batch.BatchConsumer;
import kafka.core.consumer.batch.DefaultBatchConsumer;
import kafka.core.consumer.single.DefaultSingleConsumer;
import kafka.core.consumer.single.SingleConsumer;
import kafka.core.exception.ClientInitException;
import kafka.core.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultConsumer implements Consumer, ConfigurableConsumer
{
    public static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsumer.class);
    private final HLClientConfig config;
    private final Producer producer;
    private final Receiver receiver;
    private final Recorder recorder;
    private SingleConsumer singleConsumer;
    private BatchConsumer batchConsumer;
    private final AtomicBoolean isRunning;

    public DefaultConsumer(HLClientConfig config, Producer producer, Receiver receiver, Recorder recorder)
    {
        this.config = config;
        this.producer = producer;
        this.receiver = receiver;
        this.recorder = recorder;

        this.isRunning = new AtomicBoolean(false);
    }

    public void start()
    {
        try
        {
            LOGGER.info("[HL_MESSAGE] Starting consumer...");
            if (this.isRunning.compareAndSet(false, true))
            {
                this.singleConsumer = new DefaultSingleConsumer(this.config, this.producer);
                this.batchConsumer = new DefaultBatchConsumer(this.config, this.singleConsumer, this.receiver, this.recorder);

                this.singleConsumer.start();
                this.batchConsumer.start();

                LOGGER.info("[HL_MESSAGE] Consumer is successfully started.");
            }
            else
            {
                LOGGER.warn("[HL_MESSAGE] Consumer is already running.");
            }
        }
        catch (Exception e)
        {
            throw new ClientInitException("[HL_MESSAGE] Consumer initialization failed.", e);
        }
    }

    public void close()  throws IOException
    {
        LOGGER.info("[HL_MESSAGE] Closing consumer...");
        if (this.isRunning.compareAndSet(true, false))
        {
            this.singleConsumer.close();
            this.batchConsumer.close();
            LOGGER.info("[HL_MESSAGE] Consumer is successfully closed.");
        }
        else
        {
            LOGGER.warn("[HL_MESSAGE] Consumer is not running.");
        }
    }

    public void setConsumeThread(int threadNum)
    {
        this.singleConsumer.setConsumeThread(threadNum);
    }
}

