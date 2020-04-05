package kafka.core.producer.event;

import kafka.core.engine.Event;

public class InitCompensateEvent implements Event {
    private int batchSize;

    public InitCompensateEvent(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
