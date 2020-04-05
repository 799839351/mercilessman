package kafka.core.producer;


import kafka.core.HLClientConfig;
import kafka.core.Message;
import kafka.core.component.Sender;
import kafka.core.component.Storage;
import kafka.core.component.ValueSerializer;
import kafka.core.engine.DefaultEventBroadcaster;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ClientInitException;
import kafka.core.exception.ProduceException;
import kafka.core.producer.event.PreSendingEvent;
import kafka.core.producer.listener.*;
import kafka.core.producer.processor.*;
import kafka.core.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultProducer implements Producer, Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(DefaultProducer.class);
    protected AtomicBoolean running;
    private final EventBroadcaster broadcaster;
    private final HLClientConfig config;
    private final Sender sender;
    private final Storage storage;
    private boolean enableCompensate;
    private boolean enableVerboseLog;
    private ProduceProcessor userPreProcessor;
    private ProduceProcessor userPostProcessor;
    private ValueSerializer valueSerializer;
    private int eventQueueSize;
    private int eventThreadNum;
    private PreSendProcessListener preSendProcessListener;
    private SendingListener sendingListener;
    private PostSendProcessListener postSendProcessListener;
    private ProduceFinishListener produceFinishListener;
    private SendingErrorListener sendingErrorListener;
    private CompensateListener compensateListener;
    private CompensateInitiator compensateInitiator;

    public DefaultProducer(HLClientConfig config, Sender sender, Storage storage)
            throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        this.running = new AtomicBoolean(false);

        this.config = config;
        this.sender = sender;
        this.storage = storage;

        this.enableCompensate = ConfigUtils.getBoolean(config.get("producer.enable.compensate")).booleanValue();
        this.enableVerboseLog = ConfigUtils.getBoolean(config.get("client.log.verbose")).booleanValue();
        this.userPreProcessor = ((ProduceProcessor) ConfigUtils.getInstance(ProduceProcessor.class, config.get("producer.pre.processor")));

        this.userPostProcessor = ((ProduceProcessor) ConfigUtils.getInstance(ProduceProcessor.class, config.get("producer.post.processor")));

        this.valueSerializer = ((ValueSerializer) ConfigUtils.getInstance(ValueSerializer.class, config.get("producer.value.serializer")));

        this.eventQueueSize = ConfigUtils.getInteger(config.get("producer.event.queue.size")).intValue();
        this.eventThreadNum = ConfigUtils.getInteger(config.get("producer.event.thread.num")).intValue();

        this.broadcaster = new DefaultEventBroadcaster("hl-producer", this.eventThreadNum, this.eventQueueSize);
    }

    public void start() {
        try {
            LOGGER.info("[HL_MESSAGE] Starting Producer...");
            if (this.running.compareAndSet(false, true)) {
                initPreProcessListener();

                initSendListener();

                initPostProcessListener();

                initErrorHandlingListener();

                initProduceFinishListener();

                initCompensateListener();

                startCompensate();

                LOGGER.info("[HL_MESSAGE] Producer successfully started.");
            } else {
                LOGGER.warn("[HL_MESSAGE] Producer is already started.");
            }
        } catch (Exception e) {
            throw new ClientInitException("[HL_MESSAGE] Producer initialization failed.", e);
        }
    }

    private void initPreProcessListener() {
        this.preSendProcessListener = new PreSendProcessListener(this.config);
        this.preSendProcessListener.addProcessor(ProduceStatus.PRE_PROCESS_CALLBACK, new PreProcessCallbackProcessor());
        if (null != this.userPreProcessor) {
            this.preSendProcessListener.addProcessor(ProduceStatus.USER_PREPROCESSOR, this.userPreProcessor);
        }
        this.preSendProcessListener.addProcessor(ProduceStatus.SERIALIZE, new SerializeProcessor(this.valueSerializer));
        if (this.enableCompensate) {
            this.preSendProcessListener.addProcessor(ProduceStatus.LOCAL_STORE, new LocalStoreProcessor(this.storage));
        }
        this.broadcaster.register(this.preSendProcessListener, this.preSendProcessListener.getListeningEvents());
    }

    private void initSendListener() {
        this.sendingListener = new SendingListener(this.sender);
        this.broadcaster.register(this.sendingListener, this.sendingListener.getListeningEvents());
    }

    private void initPostProcessListener() {
        this.postSendProcessListener = new PostSendProcessListener(this.config);
        if (this.enableCompensate) {
            this.postSendProcessListener.addProcessor(ProduceStatus.LOCAL_COMMIT, new LocalCommitProcessor(this.storage));
        }
        if (null != this.userPostProcessor) {
            this.postSendProcessListener.addProcessor(ProduceStatus.USER_POSTPROCESSOR, this.userPostProcessor);
        }
        this.postSendProcessListener.addProcessor(ProduceStatus.POST_PROCESS_CALLBACK, new PostProcessCallbackProcessor());

        this.broadcaster.register(this.postSendProcessListener, this.postSendProcessListener.getListeningEvents());
    }

    private void initErrorHandlingListener() {
        this.sendingErrorListener = new SendingErrorListener(this.config);
        this.broadcaster.register(this.sendingErrorListener, this.sendingErrorListener.getListeningEvents());
    }

    private void initProduceFinishListener() {
        this.produceFinishListener = new ProduceFinishListener(this.config);
        this.broadcaster.register(this.produceFinishListener, this.produceFinishListener.getListeningEvents());
    }

    private void initCompensateListener() {
        if (this.enableCompensate) {
            this.compensateListener = new CompensateListener(this.config, this.storage);
            this.broadcaster.register(this.compensateListener, this.compensateListener.getListeningEvents());
        }
    }

    private void startCompensate() {
        if (this.enableCompensate) {
            LOGGER.info("[HL_MESSAGE] Compensator initiated...");
            this.compensateInitiator = new CompensateInitiator(this.config, this.broadcaster);
            this.compensateInitiator.start();
        } else {
            LOGGER.info("[HL_MESSAGE] Compensator has been disabled...");
        }
    }

    public ProduceFuture send(Message<?> message) {
        return send(message, null);
    }

    public ProduceFuture send(Message<?> message, ProduceCallback callback) {
        try {
            assertRunning();
            ProduceContext produceContext = new ProduceContext(message);
            produceContext.setProduceCallback(callback);
            this.broadcaster.publish(new PreSendingEvent(produceContext), true);
            return new ProduceFuture(produceContext);
        } catch (RejectedExecutionException e) {
            throw new ProduceException("[HL_MESSAGE] Submitting message has failed because the event queue is full. This is probably caused by too many tasks being timed out. Please check the availability of your target message server. ");
        } catch (Exception e) {
            throw new ProduceException("[HL_MESSAGE] Unknown error happened when submitting this message.", e);
        }
    }

    public void close() throws IOException {
        LOGGER.info("[HL_MESSAGE] Closing producer...");
        if (this.running.compareAndSet(true, false)) {
            this.compensateInitiator.close();
            this.sender.close();
            this.storage.close();
            this.broadcaster.close();
            LOGGER.info("[HL_MESSAGE] Producer is successfully closed.");
        } else {
            LOGGER.warn("[HL_MESSAGE] Producer is not running.");
        }
    }

    private void assertRunning() {
        if (!this.running.get()) {
            throw new IllegalStateException("This producer is not started or has been closed.");
        }
    }
}

