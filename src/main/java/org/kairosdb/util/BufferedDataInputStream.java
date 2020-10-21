package org.kairosdb.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by bhawkins on 12/10/13.
 */
public class BufferedDataInputStream extends KDataInputStream {
    /**
     * @param file
     * @param startPosition
     * @param size          Size of stream buffer
     */
    public BufferedDataInputStream(final RandomAccessFile file, final long startPosition, final int size) {
        super(new BufferedInputStream(new WrappedInputStream(file, startPosition), size));
    }

    private static class WrappedInputStream extends InputStream {
        private FileChannel m_file;
        private long m_position;

        public WrappedInputStream(final RandomAccessFile file, final long startPosition) {
            m_file = file.getChannel();
            m_position = startPosition;
        }

        @Override
        public int read() throws IOException {
            return -1;
        }

        @Override
        public int read(final byte[] dest, final int offset, final int length) throws IOException {
            final ByteBuffer buffer = ByteBuffer.wrap(dest, offset, length);

            final int read = m_file.read(buffer, m_position);
            m_position += read;

            return (read);
        }

        @Override
        public void close() throws IOException {
            //Nothing to do, m_file is a shared resource closed at a higher level
            m_file = null;
        }
    }
}
