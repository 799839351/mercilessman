package kafka.core.producer;


public enum ProduceStatus {
    ERROR_HANDLING,
    INIT,
    PRE_PROCESS_CALLBACK,
    USER_PREPROCESSOR,
    SERIALIZE,
    LOCAL_STORE,
    COMPENSATE,
    SEND,
    LOCAL_COMMIT,
    USER_POSTPROCESSOR,
    POST_PROCESS_CALLBACK,
    FINISH,
    END;

    private ProduceStatus() {
    }
}

