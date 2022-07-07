package org.simplestorage4j.azure.datalake;

import com.azure.storage.file.datalake.DataLakeFileClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

import lombok.val;

public class AzFileExclusiveOutputStream extends OutputStream {
    
    private final DataLakeFileClient fileClient;
    private long currFilePosition;
    
    public AzFileExclusiveOutputStream(DataLakeFileClient fileClient, long currFilePosition) {
        this.fileClient = fileClient;
        this.currFilePosition = currFilePosition;
    }

    @Override
    public void write(int b) throws IOException {
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
        val in = new ByteArrayInputStream(b, off, len);
        fileClient.append(in, currFilePosition, len);
        currFilePosition += len;
    }

    @Override
    public void flush() throws IOException {
        fileClient.flush(currFilePosition);
    }
    
}