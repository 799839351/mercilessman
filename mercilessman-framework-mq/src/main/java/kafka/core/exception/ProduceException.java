package kafka.core.exception;

public class ProduceException extends RuntimeException {
    public ProduceException() {
    }

    public ProduceException(String message) {
        super(message);
    }

    public ProduceException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProduceException(Throwable cause) {
        super(cause);
    }
}

