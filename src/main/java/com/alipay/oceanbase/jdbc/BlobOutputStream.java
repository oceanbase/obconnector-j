package com.alipay.oceanbase.jdbc;

import java.io.IOException;
import java.io.OutputStream;

/** Output stream for the blob. */
class BlobOutputStream extends OutputStream {

    private final OceanBaseBlob blob;
    private int                 pos;

    public BlobOutputStream(OceanBaseBlob blob, int pos) {
        this.blob = blob;
        this.pos = pos;
    }

    @Override
    public void write(int bit) throws IOException {

        if (this.pos >= blob.length) {
            byte[] tmp = new byte[2 * blob.length + 1];
            System.arraycopy(blob.data, blob.offset, tmp, 0, blob.length);
            blob.data = tmp;
            pos -= blob.offset;
            blob.offset = 0;
            blob.length++;
        }
        blob.data[pos] = (byte) bit;
        pos++;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        if (off < 0) {
            throw new IOException("Invalid offset " + off);
        }
        int realLen = Math.min(buf.length - off, len);
        if (pos + realLen >= blob.length) {
            int newLen = 2 * blob.length + realLen;
            byte[] tmp = new byte[newLen];
            System.arraycopy(blob.data, blob.offset, tmp, 0, blob.length);
            blob.data = tmp;
            pos -= blob.offset;
            blob.offset = 0;
            blob.length = pos + realLen;
        }
        System.arraycopy(buf, off, blob.data, pos, realLen);
        pos += realLen;
    }

    @Override
    public void write(byte[] buf) throws IOException {
        write(buf, 0, buf.length);
    }
}
