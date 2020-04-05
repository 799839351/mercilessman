package kafka.core.consumer.batch;

public enum BatchStatus {
    ERROR_HANDLING,
    INIT,
    PREPARE,
    POLL,
    TRANSFER,
    AWAIT,
    FINISH,
    END;

    private BatchStatus() {
    }
}

