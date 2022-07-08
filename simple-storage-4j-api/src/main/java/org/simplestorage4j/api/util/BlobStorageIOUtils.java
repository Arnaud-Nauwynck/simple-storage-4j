package org.simplestorage4j.api.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.iocost.counter.BlobStorageIOTimeCounter;

import lombok.val;

public class BlobStorageIOUtils {

	public static final int DEFAULT_BUFFER_SIZE = 8192;
	
	// deprecated.. cf common-io IOUtils, prefer use skip(.. BlobStorageOperationCounter)
	public static void skipFully(InputStream in, final long len) throws IOException {
		int nSkip = 0;
		while(nSkip < len) {
			long count = in.skip(len - nSkip);
			if (count < 0) {
				throw new EOFException();
			}
			nSkip += count;
		}
	}

	// deprecated.. cf common-io IOUtils, prefer use copy(.. BlobStorageOperationCounter)
	public static long copy(final InputStream input, final OutputStream output) throws IOException {
    	return copy(input, output, new byte[DEFAULT_BUFFER_SIZE]);
    }

	// deprecated.. cf common-io IOUtils, prefer use copy(.. BlobStorageOperationCounter)
    public static long copy(final InputStream input, final OutputStream output, final byte[] buffer) throws IOException {
        long count = 0;
        int n;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    // deprecated.. cf common-io IOUtils, prefer use readFully(.. BlobStorageOperationCounter)
    public static byte[] readFully(InputStream in, int len) throws IOException {
		byte[] res = new byte[len];
		int n = 0;
		while (n < len) {
		    int count = in.read(res, n, len - n);
		    if (count < 0) {
		        throw new EOFException(); // should not occur
		    }
		    n += count;
		}
		return res;
	}

	// ------------------------------------------------------------------------
	
    public static long copy(
    		final @Nonnull InputStream input, final @Nonnull BlobStorageIOTimeCounter inputIOCounter, 
    		final @Nonnull OutputStream output, final @Nonnull BlobStorageIOTimeCounter outputIOCounter
    		) throws IOException {
    	return copy(input, inputIOCounter, output, outputIOCounter,
    			new byte[DEFAULT_BUFFER_SIZE]);
    }
	
    public static long copy(
    		final @Nonnull InputStream input, final @Nonnull BlobStorageIOTimeCounter inputIOCounter, 
    		final @Nonnull OutputStream output, final @Nonnull BlobStorageIOTimeCounter outputIOCounter,
    		final byte[] buffer) throws IOException {
        long count = 0;
        for(;;) {
        	val startReadTime = System.currentTimeMillis(); 
        	
        	// read
        	int n = input.read(buffer);
        	
        	val readMillis = System.currentTimeMillis() - startReadTime; 
        	inputIOCounter.incr(readMillis, n, 0L, 1, 0, 0);
        	if (n == -1) {
        		break;
        	}

        	val startWriteTime = System.currentTimeMillis(); 

        	// write
        	output.write(buffer, 0, n);
        	
        	val writeMillis = System.currentTimeMillis() - startWriteTime; 
        	outputIOCounter.incr(writeMillis, 0L, n, 1, 0, 0);
        	
            count += n;
        }
        return count;
    }
    
    public static byte[] readFully(final @Nonnull InputStream in, final int len,
    		final @Nonnull BlobStorageIOTimeCounter inputIOCounter
    		) throws IOException {
		byte[] res = new byte[len];
		int n = 0;
		while (n < len) {
        	val startReadTime = System.currentTimeMillis(); 

        	int count = in.read(res, n, len - n);
        	
        	val readMillis = System.currentTimeMillis() - startReadTime; 
        	inputIOCounter.incr(readMillis, n, 0L, 1, 0, 0);
        	
		    if (count < 0) {
		        throw new EOFException(); // should not occur
		    }
		    n += count;
		}
		return res;
	}

}
