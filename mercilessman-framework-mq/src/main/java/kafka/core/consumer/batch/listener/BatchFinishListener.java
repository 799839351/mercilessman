package kafka.core.consumer.batch.listener;


import kafka.core.HLClientConfig;
import kafka.core.component.Receiver;
import kafka.core.component.Recorder;
import kafka.core.consumer.batch.BatchContext;
import kafka.core.consumer.batch.BatchStatus;
import kafka.core.consumer.batch.event.BatchFinishEvent;
import kafka.core.consumer.batch.event.PrepareBatchEvent;
import kafka.core.engine.AbstractListener;
import kafka.core.engine.BroadcasterAware;
import kafka.core.engine.EventBroadcaster;
import kafka.core.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class BatchFinishListener extends AbstractListener<BatchFinishEvent> implements BroadcasterAware {
    public static final Logger LOGGER = LoggerFactory.getLogger(BatchFinishListener.class);
    private final HLClientConfig config;
    private final AtomicBoolean busy;
    private final Receiver receiver;
    private final Recorder recorder;
    private EventBroadcaster broadcaster;
    private final boolean enablePollCycle;

    public BatchFinishListener(HLClientConfig config, AtomicBoolean busy, Receiver receiver, Recorder recorder) {
        addListeningEvent(BatchFinishEvent.class);

        this.config = config;
        this.busy = busy;
        this.receiver = receiver;
        this.recorder = recorder;

        this.enablePollCycle = ConfigUtils.getBoolean(config.get("consumer.enable.poll.cycle")).booleanValue();
    }

    public void process(BatchFinishEvent event) {
        try {
            BatchContext context = event.getContext();
            context.setStatus(BatchStatus.FINISH);
            if (context.getMessageCount().intValue() > 0) {
                this.receiver.commit(this.recorder);
                this.recorder.reset();
            }
            if (!this.busy.compareAndSet(true, false)) {
                LOGGER.error("[HL_MESSAGE] Batch consumer is unexpectedly set to idle, something must be very wrong.");
            }
            context.setStatus(BatchStatus.END);
            if ((context.getMessageCount().intValue() > 0) && (this.enablePollCycle)) {
                this.broadcaster.publish(new PrepareBatchEvent());
            }
        } catch (Exception e) {
            this.recorder.reset();
            this.busy.set(false);
            event.getContext().setStatus(BatchStatus.END);
            LOGGER.error("[HL_MESSAGE] Finishing batch failed, aborting this batch: " + event.getContext(), e);
            this.broadcaster.publish(new PrepareBatchEvent());
        }
    }

    public void setBroadcaster(EventBroadcaster eventBroadcaster) {
        this.broadcaster = eventBroadcaster;
    }
}

