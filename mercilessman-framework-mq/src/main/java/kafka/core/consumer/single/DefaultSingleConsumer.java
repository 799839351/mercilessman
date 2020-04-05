package kafka.core.consumer.single;


import kafka.core.HLClientConfig;
import kafka.core.consumer.ConsumeContext;
import kafka.core.consumer.single.event.PreProcessEvent;
import kafka.core.consumer.single.listener.*;
import kafka.core.engine.DefaultEventBroadcaster;
import kafka.core.engine.EventBroadcaster;
import kafka.core.exception.ClientInitException;
import kafka.core.producer.Producer;
import kafka.core.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DefaultSingleConsumer implements SingleConsumer {
    public static final Logger LOGGER = LoggerFactory.getLogger(DefaultSingleConsumer.class);
    private final EventBroadcaster broadcaster;
    private final HLClientConfig config;
    private final Producer producer;
    private PreProcessListener preProcessListener;
    private DeserializeListener deserializeListener;
    private DispatchListener dispatchListener;
    private PostProcessListener postProcessListener;
    private ReproduceListener reproduceListener;
    private ConsumeFinishListener consumeFinishListener;
    private ConsumeErrorListener consumeErrorListener;
    private int consumeThread;

    public DefaultSingleConsumer(HLClientConfig config, Producer producer) {
        this.config = config;
        this.producer = producer;

        this.consumeThread = ConfigUtils.getInteger(config.get("consumer.thread.num")).intValue();
        this.broadcaster = new DefaultEventBroadcaster("hl-single-consumer", this.consumeThread, 4096);
    }

    public void start() {
        try {
            this.preProcessListener = new PreProcessListener(this.config);
            this.deserializeListener = new DeserializeListener(this.config);
            this.dispatchListener = new DispatchListener(this.config);
            this.postProcessListener = new PostProcessListener(this.config);
            this.reproduceListener = new ReproduceListener(this.config, this.producer);
            this.consumeFinishListener = new ConsumeFinishListener(this.config);
            this.consumeErrorListener = new ConsumeErrorListener(this.config);

            this.broadcaster.register(this.preProcessListener, this.preProcessListener.getListeningEvents());
            this.broadcaster.register(this.deserializeListener, this.deserializeListener.getListeningEvents());
            this.broadcaster.register(this.dispatchListener, this.dispatchListener.getListeningEvents());
            this.broadcaster.register(this.postProcessListener, this.postProcessListener.getListeningEvents());
            this.broadcaster.register(this.reproduceListener, this.reproduceListener.getListeningEvents());
            this.broadcaster.register(this.consumeFinishListener, this.consumeFinishListener.getListeningEvents());
            this.broadcaster.register(this.consumeErrorListener, this.consumeErrorListener.getListeningEvents());
        } catch (Exception e) {
            throw new ClientInitException("[HL_MESSAGE] Consumer initialization failed...", e);
        }
    }

    public void offerMessage(ConsumeContext context, ConsumeCallback callback) {
        context.setCallback(callback);
        this.broadcaster.publish(new PreProcessEvent(context));
    }

    public void setConsumeThread(int threadNum) {
        this.broadcaster.setThread(threadNum);
    }

    public void close() throws IOException {
        this.broadcaster.close();
    }
}

