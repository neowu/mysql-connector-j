/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol;

import java.io.IOException;
import java.io.InputStream;

/**
 * A non-blocking buffered input stream. Reads more if it can, won't block to fill the buffer, only blocks to satisfy a request of read(byte[])
 */
public class ReadAheadInputStream extends InputStream {

    private final static int DEFAULT_BUFFER_SIZE = 4096;

    private InputStream underlyingStream;

    private byte buf[];

    protected int endOfCurrentData;

    protected int currentPosition;

    private void fill(int readAtLeastTheseManyBytes) throws IOException {
        checkClosed();

        this.currentPosition = 0; /* no mark: throw away the buffer */

        this.endOfCurrentData = this.currentPosition;

        // Read at least as many bytes as the caller wants, but don't block to fill the whole buffer (like java.io.BufferdInputStream does)

        int bytesToRead = Math.min(this.buf.length - this.currentPosition, readAtLeastTheseManyBytes);

        int bytesAvailable = this.underlyingStream.available();

        if (bytesAvailable > bytesToRead) {

            // Great, there's more available, let's grab those bytes too! (read-ahead)

            bytesToRead = Math.min(this.buf.length - this.currentPosition, bytesAvailable);
        }

        int n = this.underlyingStream.read(this.buf, this.currentPosition, bytesToRead);

        if (n > 0) {
            this.endOfCurrentData = n + this.currentPosition;
        }
    }

    private int readFromUnderlyingStreamIfNecessary(byte[] b, int off, int len) throws IOException {
        checkClosed();

        int avail = this.endOfCurrentData - this.currentPosition;

        if (avail <= 0) {

            if (len >= this.buf.length) {
                return this.underlyingStream.read(b, off, len);
            }

            fill(len);

            avail = this.endOfCurrentData - this.currentPosition;

            if (avail <= 0) {
                return -1;
            }
        }

        int bytesActuallyRead = avail < len ? avail : len;

        System.arraycopy(this.buf, this.currentPosition, b, off, bytesActuallyRead);

        this.currentPosition += bytesActuallyRead;

        return bytesActuallyRead;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        checkClosed(); // Check for closed stream
        if ((off | len | off + len | b.length - (off + len)) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int totalBytesRead = 0;

        while (true) {
            int bytesReadThisRound = readFromUnderlyingStreamIfNecessary(b, off + totalBytesRead, len - totalBytesRead);

            // end-of-stream?
            if (bytesReadThisRound <= 0) {
                if (totalBytesRead == 0) {
                    totalBytesRead = bytesReadThisRound;
                }

                break;
            }

            totalBytesRead += bytesReadThisRound;

            // Read _at_least_ enough bytes
            // Nothing to read?
            if (totalBytesRead >= len || this.underlyingStream.available() <= 0) {
                break;
            }
        }

        return totalBytesRead;
    }

    @Override
    public int read() throws IOException {
        checkClosed();

        if (this.currentPosition >= this.endOfCurrentData) {
            fill(1);
            if (this.currentPosition >= this.endOfCurrentData) {
                return -1;
            }
        }

        return this.buf[this.currentPosition++] & 0xff;
    }

    @Override
    public int available() throws IOException {
        checkClosed();

        return this.underlyingStream.available() + this.endOfCurrentData - this.currentPosition;
    }

    private void checkClosed() throws IOException {
        if (this.buf == null) {
            throw new IOException("Stream closed");
        }
    }

    public ReadAheadInputStream(InputStream toBuffer, int bufferSize) {
        this.underlyingStream = toBuffer;
        this.buf = new byte[bufferSize];
    }

    @Override
    public void close() throws IOException {
        if (this.underlyingStream != null) {
            try {
                this.underlyingStream.close();
            } finally {
                this.underlyingStream = null;
                this.buf = null;
            }
        }
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        if (n <= 0) {
            return 0;
        }

        long bytesAvailInBuffer = this.endOfCurrentData - this.currentPosition;

        if (bytesAvailInBuffer <= 0) {

            fill((int) n);
            bytesAvailInBuffer = this.endOfCurrentData - this.currentPosition;
            if (bytesAvailInBuffer <= 0) {
                return 0;
            }
        }

        long bytesSkipped = bytesAvailInBuffer < n ? bytesAvailInBuffer : n;
        this.currentPosition += bytesSkipped;
        return bytesSkipped;
    }

}
