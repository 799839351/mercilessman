package kafka.core.producer;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ProduceFuture implements Future<ProduceResult> {
    private final ProduceContext context;

    public ProduceFuture(ProduceContext context) {
        this.context = context;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.context.cancel();
    }

    public boolean isCancelled() {
        return this.context.isCanceled();
    }

    public boolean isDone() {
        return this.context.isConfirmed();
    }

    public ProduceResult get()
            throws InterruptedException, ExecutionException {
        this.context.waitTillDone();
        ProduceResult result = getResult();
        if (!result.isSuccess()) {
            throw new ExecutionException("[HL_MESSAGE] Send message failed.", result.getException());
        }
        return result;
    }

    public ProduceResult get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!this.context.waitTillDone(timeout, unit)) {
            throw new TimeoutException("[HL_MESSAGE] Awaiting produce has timed out.");
        }
        ProduceResult result = getResult();
        if (!result.isSuccess()) {
            throw new ExecutionException("[HL_MESSAGE] Send message failed.", result.getException());
        }
        return result;
    }

    private ProduceResult getResult() {
        ProduceResult result = new ProduceResult(this.context.judgeSuccess(), this.context.getLastStatus());
        result.setException(this.context.getProduceException());
        result.setErrorStatus(this.context.getErrorStatus());
        result.setMetadata(this.context.getSendMetadata());
        return result;
    }

    public void setWaitStage(ProduceStatus status) {
        this.context.setConfirmStatus(status);
    }
}

