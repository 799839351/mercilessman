package kafka.core.producer.listener;


import kafka.core.HLClientConfig;
import kafka.core.engine.AbstractListener;
import kafka.core.producer.ProduceStatus;
import kafka.core.producer.event.ProduceFinishEvent;

public class ProduceFinishListener extends AbstractListener<ProduceFinishEvent> {
    public ProduceFinishListener(HLClientConfig config) {
        addListeningEvent(ProduceFinishEvent.class);
    }

    public void process(ProduceFinishEvent event) {
        event.getContext().setLastStatus(ProduceStatus.FINISH);

        event.getContext().setLastStatus(ProduceStatus.END);
    }
}
