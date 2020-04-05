package kafka.techvalid.appender.file;


import kafka.techvalid.appender.log.LogRecorder;
import org.apache.commons.io.IOUtils;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class AppendFile implements Closeable {
    private File file;
    private RandomAccessFile randomAccessFile;
    private RandomAccessFile readFile;
    private FileChannel channel;
    private ByteBuffer buffer;
    private boolean created;
    private long expectedPos = 0L;

    public AppendFile(File file) throws IOException {
        this.file = file;
        this.created = file.createNewFile();
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.channel = this.randomAccessFile.getChannel();
        this.buffer = ByteBuffer.allocateDirect(4);
        this.channel.position(this.channel.size());
        this.expectedPos = this.channel.position();
    }

    public boolean isCreated() {
        return this.created;
    }

    public synchronized long append(ByteBuffer buffer)
            throws IOException {
        long pos = this.channel.position();
        if (pos < this.expectedPos) {
            LogRecorder.getInstance().error("system enforce file ptr go from [" + this.expectedPos + "] to [" + pos + "]. A enforce fix" + " will be executed.");

            pos = this.expectedPos;
            this.channel.position(pos);
        }
        this.buffer.rewind();
        this.buffer.putInt(buffer.remaining());
        this.buffer.rewind();
        this.channel.write(this.buffer);
        this.channel.write(buffer);
        this.expectedPos = this.channel.position();
        if (this.expectedPos != pos + buffer.limit() + this.buffer.limit()) {
            long realPos = pos + buffer.limit() + this.buffer.limit();
            LogRecorder.getInstance().error("system enforce file ptr go from [" + this.expectedPos + "] to [" + realPos + "]. A enforce" + " fix will be executed.");

            this.expectedPos = realPos;
        }
        return pos;
    }

    public void flush()
            throws IOException {
        this.channel.force(false);
    }

    public void close() {
        if ((this.buffer instanceof DirectBuffer)) {
            ((DirectBuffer) this.buffer).cleaner().clean();
        }
        try {
            if (this.channel != null) {
                this.channel.force(true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(this.channel);
            IOUtils.closeQuietly(this.randomAccessFile);
            IOUtils.closeQuietly(this.readFile);
            this.buffer = null;
            this.channel = null;
            this.randomAccessFile = null;
            this.readFile = null;
        }
    }

    public synchronized ByteBuffer loadOffset(int offset, int expectedLength)
            throws IOException {
        if (this.readFile == null) {
            this.readFile = new RandomAccessFile(this.file, "r");
        }
        long init = this.readFile.getFilePointer();
        this.readFile.seek(offset);
        int length = this.readFile.readInt();
        if (length != expectedLength) {
            LogRecorder.getInstance().warn("loadOffset index[" + offset + "], return NULL");
            return null;
        }
        byte[] bytes = new byte[length];
        this.readFile.read(bytes);
        this.readFile.seek(init);
        return ByteBuffer.wrap(bytes);
    }
}

