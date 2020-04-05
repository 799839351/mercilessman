package kafka.core.client;


import kafka.core.HLClientConfig;
import kafka.core.component.Receiver;
import kafka.core.component.Recorder;
import kafka.core.component.Sender;
import kafka.core.component.Storage;
import kafka.core.consumer.ConfigurableConsumer;

import java.util.Map;

public class DefaultHLClient extends AbstractHLClient implements ConfigurableHLClient {
    private final HLClientConfig config;
    private Sender sender;
    private Receiver receiver;
    private Storage storage;
    private Recorder recorder;

    public DefaultHLClient(Map<String, Object> props) {
        this.config = new HLClientConfig(props);
    }

    protected HLClientConfig getConfig() {
        return this.config;
    }

    protected Sender getSender() {
        return this.sender;
    }

    public void setSender(Sender sender) {
        this.sender = sender;
    }

    protected Receiver getReceiver() {
        return this.receiver;
    }

    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    protected Storage getStorage() {
        return this.storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    protected Recorder getRecorder() {
        return this.recorder;
    }

    public void setRecorder(Recorder recorder) {
        this.recorder = recorder;
    }

    public void setConsumeThread(int threadNum) {
        if (!(this.consumer instanceof ConfigurableConsumer)) {
            throw new UnsupportedOperationException("[HL_MESSAGE] this client does not support runtime configuration.");
        }
        LOGGER.info("[HL_MESSAGE] Setting consume thread to: " + threadNum);
        ((ConfigurableConsumer) this.consumer).setConsumeThread(threadNum);
    }
}
