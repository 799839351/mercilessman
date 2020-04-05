package kafka.core.consumer.single;


public enum ConsumeStatus {

    ERROR_HANDLING,
    INIT,
    PRE_PROCESS,
    DESERIALIZE,
    DISPATCH,
    POST_PROCESS,
    REPRODUCE,
    FINISH,
    END;

    private ConsumeStatus() {
    }
}
