package com.oceanbase.jdbc;

import java.io.IOException;
import java.io.OutputStream;

public class LobOutputStream extends OutputStream {
    private final Lob lob;
    private int       pos;

    public LobOutputStream(Lob lob, int pos) {
        this.lob = lob;
        this.pos = pos;
    }

    @Override
    public void write(int bit) throws IOException {

        if (this.pos >= lob.length) {
            byte[] tmp = new byte[2 * lob.length + 1];
            System.arraycopy(lob.data, lob.offset, tmp, 0, lob.length);
            lob.data = tmp;
            pos -= lob.offset;
            lob.offset = 0;
            lob.length++;
        }
        lob.data[pos] = (byte) bit;
        pos++;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        if (off < 0) {
            throw new IOException("Invalid offset " + off);
        }
        int realLen = Math.min(buf.length - off, len);
        if (pos + realLen >= lob.length) {
            int newLen = 2 * lob.length + realLen;
            byte[] tmp = new byte[newLen];
            System.arraycopy(lob.data, lob.offset, tmp, 0, lob.length);
            lob.data = tmp;
            pos -= lob.offset;
            lob.offset = 0;
            lob.length = pos + realLen;
        }
        System.arraycopy(buf, off, lob.data, pos, realLen);
        pos += realLen;
    }

    @Override
    public void write(byte[] buf) throws IOException {
        write(buf, 0, buf.length);
    }
}
