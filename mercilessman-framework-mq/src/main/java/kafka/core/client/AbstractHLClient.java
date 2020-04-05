package kafka.core.client;


import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.component.Receiver;
import kafka.core.component.Recorder;
import kafka.core.component.Sender;
import kafka.core.component.Storage;
import kafka.core.consumer.Consumer;
import kafka.core.consumer.DefaultConsumer;
import kafka.core.exception.ClientInitException;
import kafka.core.producer.DefaultProducer;
import kafka.core.producer.ProduceCallback;
import kafka.core.producer.ProduceFuture;
import kafka.core.producer.Producer;
import kafka.core.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractHLClient implements HLClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractHLClient.class);

    private final AtomicBoolean running;
    protected Producer producer;
    protected Consumer consumer;
    private String clientId;
    private boolean enableConsumer;

    public AbstractHLClient() {
        this.running = new AtomicBoolean(false);
    }

    public void start() {
        try {
            if (this.running.compareAndSet(false, true)) {
                HLClientConfig config = getConfig();
                LOGGER.info("[HL_MESSAGE] Starting HLClient: " + config);

                this.clientId = ConfigUtils.getString(config.get("client.id"));
                this.enableConsumer = ConfigUtils.getBoolean(config.get("consumer.enable")).booleanValue();

                Sender sender = getSender();
                Storage storage = getStorage();
                sender.init(config.configs());
                storage.init(config.configs());

                DefaultProducer defaultProducer = new DefaultProducer(config, sender, storage);
                this.producer = defaultProducer;
                defaultProducer.start();
                if (this.enableConsumer) {
                    Receiver receiver = getReceiver();
                    Recorder recorder = getRecorder();
                    receiver.init(config.configs());
                    recorder.init(config.configs());

                    DefaultConsumer defaultConsumer = new DefaultConsumer(config, this.producer, receiver, recorder);
                    this.consumer = defaultConsumer;
                    defaultConsumer.start();
                }
            } else {
                LOGGER.warn("[HL_MESSAGE] This client is already running.");
            }
        } catch (Exception e) {
            ClientInitException wrappedException = (e instanceof ClientInitException) ? (ClientInitException) e : new ClientInitException("[HL_MESSAGE] HLClient initialization failed.", e);

            LOGGER.error("[HL_MESSAGE] HLClient initialization failed.", wrappedException);
            throw wrappedException;
        }
    }

    public ProduceFuture send(Message<?> message) {
        return this.producer.send(message);
    }

    public ProduceFuture send(Message<?> message, ProduceCallback callback) {
        return this.producer.send(message, callback);
    }

    public void close()  throws IOException {
        if (this.running.compareAndSet(true, false)) {
            this.producer.close();
            getSender().close();
            getStorage().close();
            if (this.enableConsumer) {
                this.consumer.close();
                getReceiver().close();
            }
            LOGGER.info("[HL_MESSAGE] Client is successfully closed.");
        } else {
            LOGGER.warn("[HL_MESSAGE] This client is already closed.");
        }
    }

    public String getClientId() {
        return this.clientId;
    }

    public boolean isRunning() {
        return this.running.get();
    }

    protected abstract HLClientConfig getConfig();

    protected abstract Sender getSender();

    protected abstract Receiver getReceiver();

    protected abstract Recorder getRecorder();

    protected abstract Storage getStorage();
}

