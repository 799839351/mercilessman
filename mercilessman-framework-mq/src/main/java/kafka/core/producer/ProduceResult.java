package kafka.core.producer;


import java.util.Map;

public class ProduceResult
{
    private final boolean success;
    private final ProduceStatus lastStatus;
    private Map<String, Object> metadata;
    private Exception exception;
    private ProduceStatus errorStatus;

    public ProduceResult(boolean success, ProduceStatus status)
    {
        this.success = success;
        this.lastStatus = status;
    }

    public boolean isSuccess()
    {
        return this.success;
    }

    public ProduceStatus getLastStatus()
    {
        return this.lastStatus;
    }

    public Map<String, Object> getMetadata()
    {
        return this.metadata;
    }

    public void setMetadata(Map<String, Object> metadata)
    {
        this.metadata = metadata;
    }

    public Exception getException()
    {
        return this.exception;
    }

    public void setException(Exception exception)
    {
        this.exception = exception;
    }

    public ProduceStatus getErrorStatus()
    {
        return this.errorStatus;
    }

    public void setErrorStatus(ProduceStatus errorStatus)
    {
        this.errorStatus = errorStatus;
    }

    public String toString()
    {
        return "ProduceResult{success=" + this.success + ", lastStatus=" + this.lastStatus + ", metadata=" + this.metadata + ", exception=" + this.exception + ", errorStatus=" + this.errorStatus + '}';
    }
}

