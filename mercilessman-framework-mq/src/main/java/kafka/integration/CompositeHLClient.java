package kafka.integration;

import kafka.core.client.DefaultHLClient;
import kafka.core.component.Receiver;
import kafka.core.component.Recorder;
import kafka.core.component.Sender;
import kafka.core.component.Storage;

import java.util.Map;
import java.util.ServiceLoader;

public class CompositeHLClient extends DefaultHLClient {
    public CompositeHLClient(Map<String, Object> config) {
        super(config);
    }

    public void start() {
        if (null == this.getSender()) {
            Sender sender = ServiceLoader.load(Sender.class).iterator().next();
            //Sender sender =new kafka.componet.sender.KafkaSender();
            this.setSender(sender);
        }

        if (null == this.getRecorder()) {
            Recorder recorder = ServiceLoader.load(Recorder.class).iterator().next();
            //Recorder recorder =new kafka.componet.recorder.KafkaRecorder();
            this.setRecorder(recorder);
        }

        if (null == this.getReceiver()) {
            Receiver receiver = ServiceLoader.load(Receiver.class).iterator().next();
            //Receiver receiver=new kafka.componet.receiver.KafkaReceiver();
            this.setReceiver(receiver);
        }

        if (null == this.getStorage()) {
            Storage storage = ServiceLoader.load(Storage.class).iterator().next();
            //Storage storage=new kafka.componet.storage.LinkedLogStorage();
            this.setStorage(storage);
        }

        super.start();
    }
}

