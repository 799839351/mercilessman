package kafka.core.consumer.batch;


import kafka.core.HLClientConfig;
import kafka.core.component.Receiver;
import kafka.core.component.Recorder;
import kafka.core.consumer.Consumable;
import kafka.core.consumer.batch.listener.*;
import kafka.core.consumer.single.SingleConsumer;
import kafka.core.engine.DefaultEventBroadcaster;
import kafka.core.engine.EventBroadcaster;
import kafka.core.util.ConfigUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultBatchConsumer implements BatchConsumer {
    private final EventBroadcaster broadcaster;
    private final HLClientConfig config;
    private final Receiver receiver;
    private final Recorder recorder;
    private final SingleConsumer singleConsumer;
    private final AtomicBoolean isBusy;
    private final AtomicBoolean isRunning;
    private BatchErrorListener batchErrorListener;
    private PrepareBatchListener prepareBatchListener;
    private PollListener pollListener;
    private TransferListener transferListener;
    private AwaitRecallListener awaitRecallListener;
    private BatchFinishListener batchFinishListener;
    private ConsumeInitiator initiator;
    private long pollInterval;
    private Map<String, Consumable<?>> consumableMap;

    public DefaultBatchConsumer(HLClientConfig config, SingleConsumer singleConsumer, Receiver receiver, Recorder recorder) {
        this.broadcaster = new DefaultEventBroadcaster("hl-batch-consumer", 1, 512);

        this.config = config;
        this.singleConsumer = singleConsumer;
        this.receiver = receiver;
        this.recorder = recorder;

        this.isBusy = new AtomicBoolean(false);
        this.isRunning = new AtomicBoolean(false);

        this.pollInterval = ConfigUtils.getLong(config.get("consumer.poll.interval.ms")).longValue();
        this.consumableMap = ((Map) this.config.get("consumer.topic.relation"));
    }

    public void start() {
        if (this.isRunning.compareAndSet(false, true)) {
            this.receiver.subscribe(this.consumableMap.keySet());

            this.batchErrorListener = new BatchErrorListener(this.config);
            this.prepareBatchListener = new PrepareBatchListener(this.config, this.isBusy, this.isRunning);
            this.pollListener = new PollListener(this.config, this.receiver);
            this.transferListener = new TransferListener(this.config, this.singleConsumer, this.recorder);
            this.awaitRecallListener = new AwaitRecallListener(this.config);
            this.batchFinishListener = new BatchFinishListener(this.config, this.isBusy, this.receiver, this.recorder);

            this.broadcaster.register(this.batchErrorListener, this.batchErrorListener.getListeningEvents());
            this.broadcaster.register(this.prepareBatchListener, this.prepareBatchListener.getListeningEvents());
            this.broadcaster.register(this.pollListener, this.pollListener.getListeningEvents());
            this.broadcaster.register(this.transferListener, this.transferListener.getListeningEvents());
            this.broadcaster.register(this.awaitRecallListener, this.awaitRecallListener.getListeningEvents());
            this.broadcaster.register(this.batchFinishListener, this.batchFinishListener.getListeningEvents());

            this.initiator = new ConsumeInitiator(this.broadcaster, this.isBusy);

            this.initiator.startConsume(this.pollInterval, TimeUnit.MILLISECONDS);
        }
    }

    public void close() throws IOException {
        if (this.isRunning.compareAndSet(true, false)) {
            this.initiator.close();

            this.broadcaster.close();
        }
    }
}

