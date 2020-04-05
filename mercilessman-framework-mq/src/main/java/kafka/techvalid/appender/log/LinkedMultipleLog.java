package kafka.techvalid.appender.log;

import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.ClosedFileSystemException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.techvalid.appender.util.FullException;
import org.apache.commons.io.IOUtils;

public class LinkedMultipleLog implements Log {
    private CompletedLogCleaner cleaner = new CompletedLogCleaner();
    private LogRecorder logRecorder;

    private class CloseTask
            implements Runnable {
        private SingleLog singleLog;

        private CloseTask(SingleLog singleLog) {
            this.singleLog = singleLog;
        }

        public void run() {
            this.singleLog.getExLock().lock();
            try {
                if (!this.singleLog.isDeleted()) {
                    this.singleLog.release();
                }
            } finally {
                this.singleLog.getExLock().unlock();
            }
        }
    }

    private static final Pattern FILE_PATTERN = Pattern.compile(".*?\\.(\\d+)\\.index$");
    private File dir;
    private long validInterval;
    private ConcurrentHashMap<Long, SingleLog> logMap = new ConcurrentHashMap();
    private final LinkedBlockingDeque<SingleLog> logQueue = new LinkedBlockingDeque();
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public LinkedMultipleLog(File dir, long validInterval, LogRecorder logRecorder)
            throws ClosedByInterruptException {
        this.dir = dir;
        this.validInterval = validInterval;
        this.logRecorder = logRecorder;
        if ((!dir.exists()) || (!dir.isDirectory())) {
            dir.mkdirs();
        }
        ArrayList<Long> idList = new ArrayList();
        for (String path : dir.list()) {
            Matcher matcher = FILE_PATTERN.matcher(path);
            if (matcher.find()) {
                String id = matcher.group(1);
                Long fileId = Long.valueOf(Long.parseLong(id));
                idList.add(fileId);
            }
        }
        Collections.sort(idList);
        for (Long id : idList) {
            SingleLog singleLog = new SingleLog(id, getFile(id.longValue()), this.cleaner, logRecorder);
            try {
                GoAheadLog log = singleLog.acquire();
                if (log == null) {
                    logRecorder.info("LinkedMultipleLog delete operation");
                    cleanup(singleLog);
                } else {
                    this.logQueue.add(singleLog);
                    this.logMap.put(id, singleLog);
                }
            } catch (Exception e) {
                logRecorder.error("LinkedMultipleLog clean up operation", e);
                cleanup(singleLog);
                singleLog.throwFile();
            } finally {
                IOUtils.closeQuietly(singleLog.softAcquire());
                singleLog.forceRelease();
            }
        }
    }

    private void cleanup(SingleLog singleLog) {
        if (null != singleLog.getRef()) {
            singleLog.getRef().clear();
            singleLog.setRef(new SoftReference(null));
        }
        this.logRecorder.info("LinkedMultipleLog clean up");
        this.logRecorder.cleanupIncrement();
    }

    public long append(ByteBuffer buffer, long timestamp) throws ClosedByInterruptException {
        boolean success = false;
        long index = 0L;

        while (!success) {
            SingleLog singleLog = this.ensureHeadSingleLog();
            singleLog.getExLock().lock();
            GoAheadLog log;
            try {
                log = singleLog.acquire();
                if (log != null) {
                    index = log.append(buffer, timestamp);
                    index = this.mapIndex(singleLog.getFileId(), index);
                    success = true;
                }
            } catch (FullException var15) {
                this.ensureNextLog(singleLog);
                this.scheduler.schedule(new LinkedMultipleLog.CloseTask(singleLog), this.validInterval, TimeUnit.SECONDS);
            } catch (ClosedFileSystemException var16) {
            } catch (Exception var17) {
                if (var17 instanceof ClosedByInterruptException) {
                    throw (ClosedByInterruptException) var17;
                }

                this.ensureNextLog(singleLog);
                this.scheduler.schedule(new LinkedMultipleLog.CloseTask(singleLog), this.validInterval, TimeUnit.SECONDS);
                this.cleanup(singleLog);
                this.logRecorder.error("append file fail: " + singleLog.getFile().getName(), var17);
                singleLog.throwFile();
            } finally {
                singleLog.getExLock().unlock();
            }
        }

        return index;
    }


    private long mapIndex(Long fileId, long index) {
        return fileId.longValue() * 4096L + index;
    }

    private long mapIndex(long index) {
        return index % 4096L;
    }

    private long mapFileId(long index) {
        return index / 4096L;
    }

    private void ensureNextLog(SingleLog singleLog) {
        synchronized (this.logQueue) {
            SingleLog last = (SingleLog) this.logQueue.getLast();
            if (singleLog.compareTo(last) == 0) {
                long newId = singleLog.getFileId().longValue() + 1L;
                last = new SingleLog(Long.valueOf(newId), getFile(newId), this.cleaner, this.logRecorder);
                this.logQueue.addLast(last);
                this.logMap.put(Long.valueOf(newId), last);
            }
        }
    }

    private SingleLog ensureHeadSingleLog() {
        SingleLog log = this.logQueue.isEmpty() ? null : (SingleLog) this.logQueue.getLast();
        if (log == null) {
            synchronized (this.logQueue) {
                log = this.logQueue.isEmpty() ? null : (SingleLog) this.logQueue.getLast();
                if (log == null) {
                    log = new SingleLog(Long.valueOf(0L), getFile(0L), this.cleaner, this.logRecorder);
                    this.logQueue.addLast(log);
                    this.logMap.put(Long.valueOf(0L), log);
                }
            }
        }
        return log;
    }

    private File getFile(long i) {
        return new File(this.dir, "map." + i);
    }

    public LogItem get(long index) {
        SingleLog singleLog = (SingleLog) this.logMap.get(Long.valueOf(mapFileId(index)));
        if (singleLog != null) {
            singleLog.getExLock().lock();
            try {
                GoAheadLog log = singleLog.softAcquire();
                return log == null ? null : log.get(mapIndex(index));
            } catch (ClosedFileSystemException e) {
                LogItem localLogItem;
                return null;
            } finally {
                singleLog.getExLock().unlock();
            }
        }
        return null;
    }

    public void setStatus(long index, int status, long timestamp)
            throws IOException {
        SingleLog singleLog = (SingleLog) this.logMap.get(Long.valueOf(mapFileId(index)));
        if (singleLog != null) {
            singleLog.getExLock().lock();
            try {
                GoAheadLog log = singleLog.softAcquire();
                if (log != null) {
                    log.setStatus(mapIndex(index), status, System.currentTimeMillis());
                    checkLog(singleLog);
                }
            } catch (ClosedFileSystemException localClosedFileSystemException) {
            } finally {
                singleLog.getExLock().unlock();
            }
        }
    }

    private void checkLog(SingleLog singleLog) {
        singleLog.getExLock().lock();
        try {
            GoAheadLog log = singleLog.softAcquire();
            if (log.isReadyToRemove()) {
                synchronized (this.logQueue) {
                    this.logMap.remove(singleLog.getFileId());
                    this.logQueue.remove(singleLog);
                }
                singleLog.delete();
                singleLog.forceRelease();
            }
        } catch (ClosedFileSystemException localClosedFileSystemException) {
        } finally {
            singleLog.getExLock().unlock();
        }
    }

    public void close()
            throws IOException {
        synchronized (this.logQueue) {
            for (SingleLog singleLog : this.logQueue) {
                singleLog.getExLock().lock();
                try {
                    IOUtils.closeQuietly(singleLog.softAcquire());
                } catch (ClosedFileSystemException localClosedFileSystemException) {
                } finally {
                    singleLog.getExLock().unlock();
                }
                this.logMap.remove(singleLog);
                this.logQueue.remove(singleLog);
            }
        }
    }

    public long flush()
            throws IOException {
        long result = 0L;
        synchronized (this.logQueue) {
            for (SingleLog singleLog : this.logQueue) {
                singleLog.getExLock().lock();
                try {
                    result = singleLog.softAcquire().flush();
                } catch (ClosedFileSystemException localClosedFileSystemException) {
                } catch (Exception localException) {
                } finally {
                    singleLog.getExLock().unlock();
                }
            }
        }
        return result;
    }

    public Long getMinUncommit()
            throws IOException {
        ensureHeadSingleLog();
        GoAheadLog log = ((SingleLog) this.logQueue.getFirst()).acquire();
        return log.getMinUncommit();
    }

    public List<LogItem> loadUncommitted(int count)
            throws IOException {
        synchronized (this.logQueue) {
            ArrayList<LogItem> result = new ArrayList();
            for (SingleLog singleLog : this.logQueue) {
                GoAheadLog log = singleLog.acquire();
                if (log != null) {
                    try {
                        List<LogItem> logItems = log.loadUncommitted(count);
                        for (LogItem logItem : logItems) {
                            long interval = System.currentTimeMillis() - logItem.getRecord().getTimestamp();
                            if ((interval > this.validInterval * 1000L) && (count > 0)) {
                                result.add(new MultipleLogItem(logItem.getRecord(), logItem.getData(), singleLog.getFileId().longValue(), logItem.getIndex()));

                                count--;
                            }
                        }
                    } catch (ClosedFileSystemException localClosedFileSystemException) {
                    }
                }
                singleLog.release();
                if (count <= 0) {
                    return result;
                }
            }
            return result;
        }
    }

    public void ensure(long index)
            throws IOException {
        SingleLog singleLog = (SingleLog) this.logMap.get(Long.valueOf(mapFileId(index)));
        if (singleLog != null) {
            GoAheadLog log = singleLog.softAcquire();
            if (log != null) {
                log.ensure(mapIndex(index));
            }
        }
    }

    public void commit(long index)
            throws IOException {
        setStatus(index, 1, System.currentTimeMillis());
    }

    public File getDir() {
        return this.dir;
    }

    public LogRecorder getLogRecorder() {
        return this.logRecorder;
    }
}
