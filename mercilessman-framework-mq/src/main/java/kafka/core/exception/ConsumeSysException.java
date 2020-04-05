package kafka.core.exception;


public class ConsumeSysException extends ConsumeException {
    public ConsumeSysException() {
    }

    public ConsumeSysException(String message) {
        super(message);
    }

    public ConsumeSysException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsumeSysException(Throwable cause) {
        super(cause);
    }
}