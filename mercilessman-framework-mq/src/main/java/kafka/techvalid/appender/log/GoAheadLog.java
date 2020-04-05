package kafka.techvalid.appender.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.ClosedFileSystemException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import kafka.techvalid.appender.file.AppendFile;
import kafka.techvalid.appender.index.Index;
import kafka.techvalid.appender.index.Record;
import kafka.techvalid.appender.util.FullException;
import kafka.techvalid.appender.util.PhantomFileException;
import org.apache.commons.io.IOUtils;

public class GoAheadLog
        implements Log
{
    private Index index;
    private AppendFile file;
    private Map<Integer, LogItem> cache;
    private Set<Integer> uncommitted;
    private File indexFile;
    private File appendFile;
    private AtomicLong ensureBound;
    private ReentrantLock ensureLock;
    private File deleteFlagFile;
    private boolean created;
    private boolean closed;

    public GoAheadLog(File baseFile)
            throws IOException, PhantomFileException
    {
        this.indexFile = new File(baseFile.getParentFile(), baseFile.getName() + ".index");
        this.appendFile = new File(baseFile.getParentFile(), baseFile.getName() + ".file");
        this.deleteFlagFile = new File(baseFile.getParentFile(), baseFile.getName() + ".delete");
        checkSafe(this.indexFile, this.appendFile, this.deleteFlagFile);
        this.index = new Index(this.indexFile);
        this.file = new AppendFile(this.appendFile);
        this.cache = new HashMap();
        this.uncommitted = new HashSet();
        if (!this.file.isCreated())
        {
            this.created = false;
            init();
        }
        else
        {
            this.created = true;
        }
        this.ensureBound = new AtomicLong(this.index.getCount().intValue());

        this.ensureLock = new ReentrantLock();
    }

    private void checkSafe(File indexFile, File appendFile, File deleteFlagFile)
            throws PhantomFileException
    {
        boolean indexFileExist = indexFile.exists();
        boolean appendFileExist = appendFile.exists();
        boolean deleteFlagFileExist = deleteFlagFile.exists();
        if ((deleteFlagFileExist) || ((indexFileExist) && (!appendFileExist)) || ((!indexFileExist) && (appendFileExist)))
        {
            indexFile.delete();
            appendFile.delete();
            deleteFlagFile.delete();
            throw new PhantomFileException();
        }
    }

    private synchronized void init()
            throws IOException
    {
        List<Record> records = this.index.loadAll();
        for (int i = 0; i < records.size(); i++)
        {
            Record record = (Record)records.get(i);
            ByteBuffer buffer = this.file.loadOffset(record.getOffset(), record.getSize());
            if (buffer != null)
            {
                this.cache.put(Integer.valueOf(i), new LogItem(record, buffer, i));
                if (record.getStatus() == 0) {
                    this.uncommitted.add(Integer.valueOf(i));
                }
            }
        }
        if (isReadyToRemove())
        {
            close();
            beforeDelete();
            delete();
            throw new PhantomFileException();
        }
    }

    public synchronized long append(ByteBuffer buffer, long timestamp)
            throws IOException, FullException
    {
        if (!this.closed)
        {
            int count = getCount();
            if (this.index.isFull()) {
                throw new FullException();
            }
            int index = this.index.getCount().intValue();
            int size = buffer.remaining();
            long offset = this.file.append(buffer);
            int status = 0;
            Record record = new Record((int)offset, size, status, timestamp);
            this.index.addRecord(record);
            this.cache.put(Integer.valueOf(index), new LogItem(record, buffer, index));
            this.uncommitted.add(Integer.valueOf(index));
            return count;
        }
        throw new ClosedFileSystemException();
    }

    public LogItem get(long index)
    {
        if (!this.closed) {
            return (LogItem)this.cache.get(Integer.valueOf((int)index));
        }
        throw new ClosedFileSystemException();
    }

    public synchronized void setStatus(long index, int status, long timestamp)
    {
        if (!this.closed)
        {
            if (status == 1) {
                this.uncommitted.remove(Integer.valueOf((int)index));
            }
            this.index.setRecord((int)index, status, timestamp);
            LogItem item = (LogItem)this.cache.get(Integer.valueOf((int)index));
            if (item != null)
            {
                item.getRecord().setStatus(status);
                item.getRecord().setTimestamp(timestamp);
            }
        }
        else
        {
            throw new ClosedFileSystemException();
        }
    }

    public synchronized boolean isReadyToRemove()
    {
        return (this.index.isFull()) && (this.uncommitted.isEmpty());
    }

    public synchronized boolean isFull()
    {
        return this.index.isFull();
    }

    public boolean isCreated()
    {
        return this.created;
    }

    public synchronized void close()
            throws IOException
    {
        if (!this.closed)
        {
            this.closed = true;
            IOUtils.closeQuietly(this.index);
            IOUtils.closeQuietly(this.file);
            this.cache.clear();
        }
    }

    public synchronized long flush()
            throws IOException
    {
        if (!this.closed)
        {
            int count = this.index.getCount().intValue();
            this.file.flush();
            this.index.flush();
            return count;
        }
        throw new ClosedFileSystemException();
    }

    public synchronized Long getMinUncommit()
            throws IOException
    {
        if (this.uncommitted.isEmpty()) {
            return null;
        }
        return Long.valueOf(((Integer)Collections.min(this.uncommitted)).longValue());
    }

    public synchronized List<LogItem> loadUncommitted(int count)
    {
        if (!this.closed)
        {
            ArrayList<LogItem> result = new ArrayList();
            if (count == 0) {
                return result;
            }
            for (Integer index : this.uncommitted)
            {
                LogItem item = (LogItem)this.cache.get(index);
                if (item != null)
                {
                    result.add(item);
                    if (result.size() == count) {
                        break;
                    }
                }
            }
            return result;
        }
        throw new ClosedFileSystemException();
    }

    public void ensure(long index)
            throws IOException
    {
        long currentBound = this.ensureBound.get();
        while (currentBound < index)
        {
            if (this.ensureBound.compareAndSet(currentBound, index))
            {
                boolean locked = this.ensureLock.tryLock();
                if (locked) {
                    try
                    {
                        if (this.ensureBound.get() == index) {
                            flush();
                        }
                    }
                    finally
                    {
                        this.ensureLock.unlock();
                    }
                }
            }
            currentBound = this.ensureBound.get();
        }
    }

    public void beforeDelete()
    {
        try
        {
            this.deleteFlagFile.createNewFile();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void delete()
    {
        this.indexFile.delete();
        this.appendFile.delete();
        this.deleteFlagFile.delete();
    }

    public synchronized int getCount()
    {
        return this.index.getCount().intValue();
    }

    protected synchronized void finalize()
            throws Throwable
    {
        if (!this.closed) {
            close();
        }
        super.finalize();
    }
}

