package kafka.techvalid.appender.file;


import org.apache.commons.io.IOUtils;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MemoryMapFile
{
    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private MappedByteBuffer buffer;
    private boolean created;

    public MemoryMapFile(File file, int size)
            throws IOException
    {
        this.file = file;
        this.created = file.createNewFile();
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.channel = this.randomAccessFile.getChannel();
        this.randomAccessFile.setLength(size);
        this.buffer = this.channel.map(FileChannel.MapMode.READ_WRITE, 0L, size);
    }

    public boolean isCreated()
    {
        return this.created;
    }

    public ByteBuffer slice(int offset, int size)
    {
        ByteBuffer buf = this.buffer.slice();
        buf.position(offset);
        buf.mark();
        buf.limit(offset + size);
        return buf;
    }

    public void close()
            throws IOException
    {
        Cleaner cleaner = null;
        if ((this.buffer instanceof DirectBuffer))
        {
            this.buffer.force();
            cleaner = ((DirectBuffer)this.buffer).cleaner();
        }
        IOUtils.closeQuietly(this.channel);
        IOUtils.closeQuietly(this.randomAccessFile);
        this.channel = null;
        this.randomAccessFile = null;
        this.buffer = null;
        if (cleaner != null) {
            cleaner.clean();
        }
    }

    public void flush()
    {
        this.buffer.force();
    }
}

