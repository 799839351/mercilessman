package kafka.techvalid.appender.log;


import java.io.IOException;
import java.util.concurrent.*;

public class CompletedLogCleaner
        implements Runnable
{
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private BlockingQueue<GoAheadLog> needDelete;
    private boolean running = true;

    public CompletedLogCleaner()
    {
        this.needDelete = new LinkedBlockingQueue();
        this.executorService.submit(this);
    }

    public boolean isRunning()
    {
        return this.running;
    }

    public void setRunning(boolean running)
    {
        this.running = running;
    }

    public void delete(GoAheadLog log)
    {
        this.needDelete.offer(log);
    }

    public void run()
    {
        while (isRunning())
        {
            GoAheadLog log = null;
            try
            {
                log = (GoAheadLog)this.needDelete.poll(100L, TimeUnit.MILLISECONDS);
                if (log != null) {
                    log.close();
                }
            }
            catch (InterruptedException e)
            {
                Thread.interrupted();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            finally
            {
                if (log != null) {
                    log.delete();
                }
            }
        }
    }

    public void close()
    {
        setRunning(false);

        this.executorService.shutdown();
        try
        {
            this.executorService.awaitTermination(30L, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        while (!this.needDelete.isEmpty())
        {
            GoAheadLog log = (GoAheadLog)this.needDelete.remove();
            try
            {
                log.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                log.delete();
            }
        }
    }
}
