package kafka.techvalid.appender.log;

import kafka.techvalid.appender.util.FullException;
import kafka.techvalid.appender.util.PhantomFileException;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MultipleLog implements Log {
    private static final Pattern filePattern = Pattern.compile(".*?\\.(\\d+)\\.index");
    private File dir;
    private Map<Long, File> fileMap;
    private Map<Long, GoAheadLog> openedMap;
    private Map<Long, GoAheadLog> headMap;
    private Map<GoAheadLog, Long> inversedOpenedMap;
    private BlockingQueue<MultipleLogItem> validLogItem;
    private GoAheadLog head;
    private GoAheadLog end;
    private CompletedLogCleaner cleaner = new CompletedLogCleaner();
    private long cursor;
    private int headMapLength = 3;
    private int validTime = 3;
    private AtomicLong checkBound;


    public MultipleLog(File dir, int headMapLength, int validTime) throws IOException {
        this.dir = dir;
        this.headMapLength = headMapLength;
        this.validTime = validTime;
        this.checkBound = new AtomicLong(System.currentTimeMillis());
        if (!dir.exists() || !dir.isDirectory()) {
            dir.mkdirs();
        }

        Map<Long, Long> maxMap = new HashMap();
        List<Long> listTemp = new ArrayList();
        String[] arr$ = dir.list();
        int len$ = arr$.length;

        int i;
        for (i = 0; i < len$; ++i) {
            String path = arr$[i];
            Matcher matcher = filePattern.matcher(path);
            if (matcher.find()) {
                String id = matcher.group(1);
                Long fileId = Long.parseLong(id);
                listTemp.add(fileId);
                File f = new File(dir, path.substring(0, path.length() - ".index".length()));
                this.fileMap.put(fileId, f);
            }
        }

        if (listTemp.size() > 0) {
            long currentFileId = (Long) Collections.max(listTemp);
            if (listTemp.size() > headMapLength) {
                for (i = 0; i < headMapLength; ++i) {
                    Long fileId = (Long) Collections.max(listTemp);
                    maxMap.put(fileId, fileId);

                    GoAheadLog log;
                    try {
                        log = new GoAheadLog((File) this.fileMap.get(fileId));
                    } catch (PhantomFileException var14) {
                        this.fileMap.remove(fileId);
                        listTemp.remove(fileId);
                        continue;
                    }

                    this.headMap.put(fileId, log);
                    this.inversedOpenedMap.put(log, fileId);
                    listTemp.remove(fileId);
                }
            } else {
                while (0 != listTemp.size()) {
                    Long fileId = (Long) Collections.max(listTemp);
                    maxMap.put(fileId, fileId);

                    GoAheadLog log;
                    try {
                        log = new GoAheadLog((File) this.fileMap.get(fileId));
                    } catch (PhantomFileException var15) {
                        this.fileMap.remove(fileId);
                        listTemp.remove(fileId);
                        continue;
                    }

                    this.headMap.put(fileId, log);
                    this.inversedOpenedMap.put(log, fileId);
                    listTemp.remove(fileId);
                }
            }

            this.head = (GoAheadLog) this.headMap.get(currentFileId);
        } else {
            this.create(0L);
            this.headMap.put(0L, this.head);
        }

    }

    public CompletedLogCleaner getCleaner() {
        return this.cleaner;
    }

    public long getCount() {
        return ((Long) this.inversedOpenedMap.get(this.head)).longValue() * 4096L + this.head.getCount();
    }

    private synchronized void create(long i)
            throws IOException {
        File f = new File(this.dir, "map." + i);
        try {
            GoAheadLog log = new GoAheadLog(f);
            if ((!log.isCreated()) && (log.isFull())) {
                log.close();
                log.beforeDelete();
                log.delete();
                create(i + 1L);
            } else {
                this.fileMap.put(Long.valueOf(i), f);
                this.headMap.put(Long.valueOf(i), log);
                this.head = log;
                this.inversedOpenedMap.put(log, Long.valueOf(i));
                if ((i - this.headMapLength >= 0L) && (this.headMap.size() >= this.headMapLength)) {
                    close((GoAheadLog) this.headMap.get(Long.valueOf(i - this.headMapLength)));
                }
            }
        } catch (PhantomFileException e) {
            create(i + 1L);
        }
    }

    public synchronized long append(ByteBuffer buffer, long timestamp)
            throws IOException {
        boolean inserted = false;
        long count = getCount();
        while (!inserted) {
            try {
                long index = this.head.append(buffer, timestamp);
                LogItem logItem = this.head.get(index);
                long fileId = ((Long) this.inversedOpenedMap.get(this.head)).longValue();
                MultipleLogItem multipleLogItem = new MultipleLogItem(logItem.getRecord(), logItem.getData(), fileId, logItem.getIndex());

                updateValidLogItem(multipleLogItem);
                inserted = true;
            } catch (FullException e) {
                create(getFileId());
            } catch (IOException e) {
                if ((e instanceof ClosedByInterruptException)) {
                    throw ((ClosedByInterruptException) e);
                }
                GoAheadLog temp = this.head;
                long fileId = ((Long) this.inversedOpenedMap.get(this.head)).longValue();
                create(getFileId() + 1L);
                this.validLogItem.clear();
                this.headMap.remove(Long.valueOf(fileId));
                this.openedMap.remove(Long.valueOf(fileId));
                this.inversedOpenedMap.remove(temp);
                this.fileMap.remove(Long.valueOf(fileId));
                temp.close();
            }
        }
        return count;
    }

    private void updateValidLogItem(MultipleLogItem multipleLogItem) {
        this.validLogItem.offer(multipleLogItem);
        long nowTime = System.currentTimeMillis();
        long bound = this.checkBound.get();
        if (this.checkBound.compareAndSet(bound, nowTime)) {
            synchronized (this.validLogItem) {
                while ((null != this.validLogItem.peek()) && (this.validLogItem.size() > 0) &&
                        (nowTime - ((MultipleLogItem) this.validLogItem.peek()).getRecord().getTimestamp() > this.validTime * 60 * 1000)) {
                    this.validLogItem.poll();
                }
            }
        }
    }

    private long getFileId() {
        return getCount() / 4096L;
    }

    public LogItem get(long index) {
        LogItem logItem = null;
        if (this.headMap.containsKey(Long.valueOf(index / 4096L))) {
            GoAheadLog log = (GoAheadLog) this.headMap.get(Long.valueOf(index / 4096L));
            logItem = log.get(index % 4096L);
        } else if (this.openedMap.containsKey(Long.valueOf(index / 4096L))) {
            GoAheadLog log = (GoAheadLog) this.openedMap.get(Long.valueOf(index / 4096L));
            logItem = log.get(index % 4096L);
        }
        return logItem;
    }

    public synchronized void setStatus(long index, int status, long timestamp)
            throws IOException {
        GoAheadLog log = (GoAheadLog) this.headMap.get(Long.valueOf(index / 4096L));
        if (null == log) {
            log = (GoAheadLog) this.openedMap.get(Long.valueOf(index / 4096L));
        }
        if (null != log) {
            log.setStatus(index % 4096L, status, timestamp);
            checkLog(log);
        }
    }

    private synchronized void checkLog(GoAheadLog log)
            throws IOException {
        if ((null != log) && (log != this.head) && (log.isReadyToRemove())) {
            Long fileId = (Long) this.inversedOpenedMap.get(log);
            log.beforeDelete();
            this.fileMap.remove(fileId);
            this.headMap.remove(fileId);
            this.openedMap.remove(fileId);
            this.inversedOpenedMap.remove(log);
            this.cleaner.delete(log);
        }
    }

    private void close(GoAheadLog log)
            throws IOException {
        if ((null != log) && (log != this.head)) {
            Long fileId = (Long) this.inversedOpenedMap.get(log);
            this.inversedOpenedMap.remove(log);
            this.headMap.remove(fileId);
            log.close();
        }
    }

    public void close()
            throws IOException {
        for (GoAheadLog log : this.inversedOpenedMap.keySet()) {
            checkLog(log);
            log.close();
        }
        this.cleaner.close();
    }

    public void commit(long index)
            throws IOException {
        setStatus(index, 1, System.currentTimeMillis());
    }

    public long flush()
            throws IOException {
        Set<GoAheadLog> allLog = new HashSet();
        for (GoAheadLog log : this.headMap.values()) {
            allLog.add(log);
        }
        for (GoAheadLog log : this.openedMap.values()) {
            allLog.add(log);
        }
        for (GoAheadLog log : allLog) {
            log.flush();
        }
        return getMinUncommit().longValue();
    }

    public synchronized Long getMinUncommit()
            throws IOException {
        List<Long> uncommitted = new ArrayList();
        for (GoAheadLog log : this.openedMap.values()) {
            Long uncommit = log.getMinUncommit();
            if (uncommit != null) {
                uncommitted.add(uncommit);
            }
        }
        return Long.valueOf(uncommitted.isEmpty() ? getCount() : ((Long) Collections.min(uncommitted)).longValue());
    }

    private void transferLogItem(List<LogItem> uncommitted, int remained, GoAheadLog log) {
        List<LogItem> logItems = log.loadUncommitted(remained);
        long fileId = ((Long) this.inversedOpenedMap.get(log)).longValue();
        for (int i = 0; i < logItems.size(); i++) {
            LogItem logItem = (LogItem) logItems.get(i);
            logItem = new MultipleLogItem(logItem.getRecord(), logItem.getData(), fileId, logItem.getIndex());
            logItems.set(i, logItem);
        }
        uncommitted.addAll(logItems);
    }

    public List<LogItem> loadUncommitted(int count) {
        List<LogItem> uncommitted = new ArrayList();
        for (GoAheadLog log : this.openedMap.values()) {
            int remained = count - uncommitted.size();
            if (remained <= 0) {
                break;
            }
            transferLogItem(uncommitted, remained, log);
        }
        if (count - uncommitted.size() > 0) {
            loadUncommittedAndOpen(uncommitted, count);
        }
        loadUncommittedReduceValidLogItem(uncommitted);
        return uncommitted;
    }

    private void loadUncommittedAndOpen(List<LogItem> uncommitted, int count) {
        int remained = count - uncommitted.size();
        while ((remained > 0) && (this.cursor <= ((Long) this.inversedOpenedMap.get(this.head)).longValue())) {
            File f = (File) this.fileMap.get(Long.valueOf(this.cursor));
            if (null != f) {
                GoAheadLog log = (GoAheadLog) this.headMap.get(Long.valueOf(this.cursor));
                if (null == log) {
                    try {
                        log = new GoAheadLog(f);
                    } catch (PhantomFileException e) {
                        this.fileMap.remove(Long.valueOf(this.cursor));
                        this.cursor += 1L;
                        break;
                    } catch (IOException e) {
                        this.fileMap.remove(Long.valueOf(this.cursor));
                        this.cursor += 1L;
                        break;
                    }
                    this.openedMap.put(Long.valueOf(this.cursor), log);
                    this.inversedOpenedMap.put(log, Long.valueOf(this.cursor));
                } else {
                    this.openedMap.put(Long.valueOf(this.cursor), log);
                }
                transferLogItem(uncommitted, remained, log);
                remained = count - uncommitted.size();
            }
            this.cursor += 1L;
        }
    }

    private void loadUncommittedReduceValidLogItem(List<LogItem> uncommitted) {
        if ((uncommitted.size() > 0) && (this.validLogItem.size() > 0)) {
            for (MultipleLogItem validLog : this.validLogItem)
            {
                long index = validLog.getIndex();
                for (LogItem log : uncommitted) {
                    if (log.getIndex() == index)
                    {
                        uncommitted.remove(log);
                        break;
                    }
                }
            }
        }
    }
    public void ensure(long index)
            throws IOException {
        GoAheadLog log = (GoAheadLog) this.headMap.get(Long.valueOf(index / 4096L));
        if (null == log) {
            log = (GoAheadLog) this.openedMap.get(Long.valueOf(index / 4096L));
        }
        if (log != null) {
            log.ensure(index % 4096L);
        }
    }

    public File getDir() {
        return this.dir;
    }

    public void setDir(File dir) {
        this.dir = dir;
    }

    public Map<Long, GoAheadLog> getOpenedMap() {
        return this.openedMap;
    }

    public void setOpenedMap(Map<Long, GoAheadLog> openedMap) {
        this.openedMap = openedMap;
    }

    public Map<Long, GoAheadLog> getHeadMap() {
        return this.headMap;
    }

    public void setHeadMap(Map<Long, GoAheadLog> headMap) {
        this.headMap = headMap;
    }

    public Map<GoAheadLog, Long> getInversedOpenedMap() {
        return this.inversedOpenedMap;
    }

    public BlockingQueue<MultipleLogItem> getValidLogItem() {
        return this.validLogItem;
    }

    public void setValidLogItem(BlockingQueue<MultipleLogItem> validLogItem) {
        this.validLogItem = validLogItem;
    }
}
