package kafka.core.consumer;

public class ConsumeResult {
    private final boolean success;

    private final String message;

    public ConsumeResult(boolean success) {
        this(success, null);
    }

    public ConsumeResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public String getMessage() {
        return this.message;
    }

    public boolean getSuccess() {
        return success;
    }

    public String toString() {
        return "ConsumeResult{success=" + this.success + ", message='" + this.message + '\'' + '}';
    }
}
