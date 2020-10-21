package org.kairosdb.util;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by bhawkins on 12/10/13.
 */
public class BufferedDataOutputStream extends DataOutputStream {
    private WrappedOutputStream m_wrappedOutputStream;

    private BufferedDataOutputStream(final WrappedOutputStream outputStream) {
        super(new BufferedOutputStream(outputStream));
    }

    public static BufferedDataOutputStream create(final RandomAccessFile file, final long startPosition) {
        final WrappedOutputStream outputStream = new WrappedOutputStream(file, startPosition);
        final BufferedDataOutputStream ret = new BufferedDataOutputStream(outputStream);
        ret.setWrappedOutputStream(outputStream);

        return ret;
    }

    private void setWrappedOutputStream(final WrappedOutputStream outputStream) {
        m_wrappedOutputStream = outputStream;
    }

    public long getPosition() {
        return m_wrappedOutputStream.getPosition();
    }

    private static class WrappedOutputStream extends OutputStream {
        private final FileChannel m_file;
        private long m_position;

        public WrappedOutputStream(final RandomAccessFile file, final long startPosition) {
            m_file = file.getChannel();
            m_position = startPosition;
        }

        public long getPosition() {
            return m_position;
        }

        @Override
        public void write(final int b) throws IOException {
        }

        @Override
        public void write(final byte[] src, final int offset, final int length) throws IOException {
            final ByteBuffer buffer = ByteBuffer.wrap(src, offset, length);
            while (buffer.hasRemaining()) {
                final int written = m_file.write(buffer, m_position);
                m_position += written;
            }
        }
    }
}
