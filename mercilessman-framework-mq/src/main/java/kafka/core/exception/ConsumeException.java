package kafka.core.exception;

public class ConsumeException extends RuntimeException {
    public ConsumeException() {
    }

    public ConsumeException(String message) {
        super(message);
    }

    public ConsumeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsumeException(Throwable cause) {
        super(cause);
    }
}
