package org.kairosdb.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by bhawkins on 12/10/13.
 */
public class KDataOutput implements DataOutput {
    private final ByteArrayOutputStream m_arrayOutputStream;
    private final DataOutputStream m_dataOutputStream;

    public KDataOutput() {
        m_arrayOutputStream = new ByteArrayOutputStream();
        m_dataOutputStream = new DataOutputStream(m_arrayOutputStream);
    }

    public byte[] getBytes() throws IOException {
        m_dataOutputStream.flush();
        return m_arrayOutputStream.toByteArray();
    }

    @Override
    public void write(final int b) throws IOException {
        m_dataOutputStream.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        m_dataOutputStream.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        m_dataOutputStream.write(b, off, len);
    }

    @Override
    public void writeBoolean(final boolean v) throws IOException {
        m_dataOutputStream.writeBoolean(v);
    }

    @Override
    public void writeByte(final int v) throws IOException {
        m_dataOutputStream.writeByte(v);
    }

    @Override
    public void writeShort(final int v) throws IOException {
        m_dataOutputStream.writeShort(v);
    }

    @Override
    public void writeChar(final int v) throws IOException {
        m_dataOutputStream.writeChar(v);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        m_dataOutputStream.writeInt(v);
    }

    @Override
    public void writeLong(final long v) throws IOException {
        m_dataOutputStream.writeLong(v);
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        m_dataOutputStream.writeFloat(v);
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        m_dataOutputStream.writeDouble(v);
    }

    @Override
    public void writeBytes(final String s) throws IOException {
        m_dataOutputStream.writeBytes(s);
    }

    @Override
    public void writeChars(final String s) throws IOException {
        m_dataOutputStream.writeChars(s);
    }

    @Override
    public void writeUTF(final String s) throws IOException {
        m_dataOutputStream.writeUTF(s);
    }
}
