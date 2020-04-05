package kafka.techvalid.appender.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.ReentrantLock;

import kafka.techvalid.appender.util.PhantomFileException;
import org.apache.commons.io.IOUtils;

public class SingleLog
        implements Comparable<SingleLog>
{
    private CompletedLogCleaner cleaner;
    private LogRecorder logRecorder;
    private volatile GoAheadLog log;
    private volatile SoftReference<GoAheadLog> ref = new SoftReference(null);
    private Long fileId;
    private File file;
    private long acquireTime;
    private volatile boolean deleted = false;
    private ReentrantLock lock = new ReentrantLock();
    private ReentrantLock exLock = new ReentrantLock();

    public SingleLog(Long fileId, File file, CompletedLogCleaner cleaner, LogRecorder logRecorder)
    {
        this.fileId = fileId;
        this.file = file;
        this.cleaner = cleaner;
        this.logRecorder = logRecorder;
    }

    public boolean isDeleted()
    {
        return this.deleted;
    }

    public ReentrantLock getExLock()
    {
        return this.exLock;
    }

    public Long getFileId()
    {
        return this.fileId;
    }

    public File getFile()
    {
        return this.file;
    }

    public GoAheadLog softAcquire()
    {
        return (GoAheadLog)this.ref.get();
    }

    public GoAheadLog acquire()
            throws IOException
    {
        GoAheadLog refValue = (GoAheadLog)this.ref.get();
        if ((this.log == null) && (refValue == null))
        {
            this.lock.lock();
            try
            {
                if ((this.log == null) && (this.ref.get() == null))
                {
                    this.log = new GoAheadLog(this.file);
                    this.ref = new SoftReference(this.log);
                    refValue = this.log;
                    this.acquireTime = System.currentTimeMillis();
                }
            }
            catch (PhantomFileException localPhantomFileException) {}finally
            {
                this.lock.unlock();
            }
        }
        if (this.log == null) {
            this.log = refValue;
        }
        return this.log;
    }

    public void release()
    {
        if (this.log != null)
        {
            this.lock.lock();
            try
            {
                this.log = null;
            }
            finally
            {
                this.lock.unlock();
            }
        }
    }

    public void forceRelease()
    {
        if (this.log != null)
        {
            this.lock.lock();
            try
            {
                this.log = null;
                this.ref.clear();
            }
            finally
            {
                this.lock.unlock();
            }
        }
    }

    public synchronized void delete()
    {
        if (!this.deleted)
        {
            this.deleted = true;
            this.lock.lock();
            try
            {
                GoAheadLog log = acquire();
                if (log != null)
                {
                    log.beforeDelete();
                    log.close();
                    this.cleaner.delete(log);
                }
                forceRelease();

                this.logRecorder.info("LinkedMultipleLog delete operation");
            }
            catch (IOException e)
            {
                this.logRecorder.warn("LinkedMultipleLog can't load. Ignore this one, next startup will fix it.", e);
            }
            finally
            {
                this.lock.unlock();
            }
        }
    }

    public SoftReference<GoAheadLog> getRef()
    {
        return this.ref;
    }

    public void setRef(SoftReference<GoAheadLog> ref)
    {
        this.ref = ref;
    }

    public void throwFile()
            throws ClosedByInterruptException
    {
        File indexFile = new File(this.file.getParentFile(), this.file.getName() + ".index");
        File appendFile = new File(this.file.getParentFile(), this.file.getName() + ".file");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String suffix = ".bak." + sdf.format(Long.valueOf(System.currentTimeMillis()));
        moveFile(indexFile, new File(indexFile.getParent(), indexFile.getName() + suffix));
        moveFile(appendFile, new File(appendFile.getParent(), appendFile.getName() + suffix));
        try
        {
            new File(this.file.getParentFile(), this.file.getName() + ".delete").createNewFile();
        }
        catch (IOException e)
        {
            this.logRecorder.error("delete error file when back up it: " + this.file.getName(), e);
        }
    }

    private void moveFile(File source, File target)
            throws ClosedByInterruptException
    {
        if ((source.exists()) &&
                (!source.renameTo(target))) {
            copyFile(source, target);
        }
    }

    private void copyFile(File source, File target)
            throws ClosedByInterruptException
    {
        FileChannel in = null;
        FileChannel out = null;
        FileInputStream inStream = null;
        FileOutputStream outStream = null;
        try
        {
            inStream = new FileInputStream(source);
            outStream = new FileOutputStream(target);
            in = inStream.getChannel();
            out = outStream.getChannel();
            in.transferTo(0L, in.size(), out);
        }
        catch (IOException e)
        {
            if ((e instanceof ClosedByInterruptException)) {
                throw ((ClosedByInterruptException)e);
            }
            this.logRecorder.error("copyFile error, " + source.getName() + " to " + target.getName(), e);
        }
        finally
        {
            IOUtils.closeQuietly(inStream);
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(outStream);
            IOUtils.closeQuietly(out);
        }
    }

    public int compareTo(SingleLog o)
    {
        return this.fileId.compareTo(o.fileId);
    }

    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SingleLog)) {
            return false;
        }
        SingleLog singleLog = (SingleLog)o;
        if (!this.file.equals(singleLog.file)) {
            return false;
        }
        if (!this.fileId.equals(singleLog.fileId)) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        int result = this.fileId.hashCode();
        result = 31 * result + this.file.hashCode();
        return result;
    }
}

