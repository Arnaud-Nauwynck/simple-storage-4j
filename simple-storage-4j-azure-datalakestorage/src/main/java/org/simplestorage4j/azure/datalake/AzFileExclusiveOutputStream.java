package org.simplestorage4j.azure.datalake;

import com.azure.storage.file.datalake.DataLakeFileClient;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzFileExclusiveOutputStream extends OutputStream implements Closeable {

    private final DataLakeFileClient fileClient;
    private long currFilePosition;

    @Getter @Setter
    private boolean logEnable;

    public AzFileExclusiveOutputStream(DataLakeFileClient fileClient, long currFilePosition) {
        this.fileClient = fileClient;
        this.currFilePosition = currFilePosition;
    }

    @Override
    public void close() {
        if (logEnable) {
            log.info("az output close " + fileClient.getFileUrl());
        }
        flush();
    }

    @Override
    public void write(int b) throws IOException {
        if (logEnable) {
            log.info("az output write 1 " + fileClient.getFileUrl());
        }
        // should not be called! appending byte 1 by 1 to remote azure file..
        byte[] data = new byte[] { (byte) b };
        val in = new ByteArrayInputStream(data);
        fileClient.append(in, currFilePosition, 1);
        currFilePosition++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (logEnable) {
            log.info("az output write " + len + " " + fileClient.getFileUrl() + " pos:" + currFilePosition);
        }
        val in = new ByteArrayInputStream(b, off, len);
        fileClient.append(in, currFilePosition, len);
        currFilePosition += len;
    }

    @Override
    public void flush() {
        if (logEnable) {
            log.info("az output flush " + currFilePosition + " " + fileClient.getFileUrl());
        }
        fileClient.flush(currFilePosition, true);
    }

}