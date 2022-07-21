package org.simplestorage4j.api.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.counter.BlobStorageIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStorageIOTimeResult;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlobStorageIOUtils {

    public static final int DEFAULT_BUFFER_SIZE = 8192;

	public static final long defaultReadBlockSize = 8 * 1024 * 1024;

	public static final int maxRetryReadRange = 5;
	
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

	public static void copyFileUsingStreaming(
			final @Nonnull BlobStoragePath inputStoragePath,
			final @Nonnull BlobStorageIOTimeCounter inputIOCounter,
			final @Nonnull BlobStoragePath outputStoragePath,
			final @Nonnull BlobStorageIOTimeCounter outputIOCounter
			) {
		try (val output = outputStoragePath.openWrite()) {
			// using streaming read ... can not retry at position, need retry from pos=0 !!
			try (val input = inputStoragePath.openRead()) {
				// equivalent to .. IOUtils.copy(input, output);
				// with IOstats per input|output
				copy(input, inputIOCounter, output, outputIOCounter);
			}
		} catch(IOException ex) {
			throw new RuntimeException("Failed copy " + inputStoragePath + " " + outputStoragePath, ex);
		}
	}
	
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

    public static void copy(
            final byte[] data,
            final @Nonnull OutputStream output, final @Nonnull BlobStorageIOTimeCounter outputIOCounter
            ) throws IOException {
        copy(data, 0, data.length, output, outputIOCounter);
    }

    public static void copy(
            final byte[] data, int fromPos, int len,
            final @Nonnull OutputStream output, final @Nonnull BlobStorageIOTimeCounter outputIOCounter
            ) throws IOException {
        val startWriteTime = System.currentTimeMillis();

        output.write(data, fromPos, len);

        val writeMillis = System.currentTimeMillis() - startWriteTime;
        outputIOCounter.incr(writeMillis, 0L, len, 1, 0, 0);
    }

    private static int maxRetry = 5;

	public static byte[] readFileWithRetry(BlobStoragePath storagePath, BlobStorageIOTimeCounter inputIOCounter) {
		return readFileWithRetry(storagePath.blobStorage, storagePath.path, inputIOCounter);
	}

    public static byte[] readFileWithRetry(BlobStorage storage, String filePath, BlobStorageIOTimeCounter inputIOCounter) {
		val startReadTime = System.currentTimeMillis();

		val content = readFileWithRetry(storage, filePath);

		val readMillis = System.currentTimeMillis() - startReadTime;
		inputIOCounter.incr(readMillis, content.length, 0L, 1, 0, 0);
		return content;
    }

    public static void writeFile(BlobStoragePath storagePath, byte[] data, BlobStorageIOTimeCounter outputIOCounter) {
		writeFile(storagePath.blobStorage, storagePath.path, data, outputIOCounter);
	}
    
    public static void writeFile(BlobStorage storage, String filePath, byte[] data, BlobStorageIOTimeCounter outputIOCounter) {
		val startWrite = System.currentTimeMillis();
		
		storage.writeFile(filePath, data);
	
		val writeMillis = System.currentTimeMillis() - startWrite;
		outputIOCounter.incr(writeMillis, 0L, data.length, 1, 0, 0);
    }
    
	public static byte[] readFileWithRetry(BlobStorage storage, String filePath) {
		byte[] res = null;
		for(int retry = 0; retry < maxRetry; retry++) {
			try {
				res = storage.readFile(filePath);
				break;
			} catch(RuntimeException ex) {
				if (retry + 1 < maxRetry) {
					log.warn("Failed read file '" + filePath + "' ..retry [" + retry + "/" + maxRetry + "] ex:" + ex.getMessage());
					try {
						Thread.sleep(100);
					} catch(InterruptedException ex2) {
					}
					continue;
				} else {
					throw ex;
				}
			}
		}
		return res;
	}

	public static List<Future<byte[]>> asyncReadFileByBlocksWithRetry(
			final BlobStoragePath storagePath, //
			final long srcFileLen, //
			final BlobStorageIOTimeCounter inputIOCounter, //			
			final ExecutorService executorService //			
			) {
		return asyncReadFileByBlocksWithRetry(storagePath.blobStorage, storagePath.path, //
				srcFileLen, inputIOCounter, executorService);
	}
	
	public static List<Future<byte[]>> asyncReadFileByBlocksWithRetry(
			final BlobStorage storage, final String filePath, //
			final long srcFileLen, //
			final BlobStorageIOTimeCounter inputIOCounter, //			
			final ExecutorService executorService //			
			) {
		val res = new ArrayList<Future<byte[]>>(); 
		long currPosition = 0;
		val readBlockSize = defaultReadBlockSize; // preferred per storage?
		for(; currPosition < srcFileLen;) {
			val nextPos = Math.min(srcFileLen, currPosition + readBlockSize); 
			
			val readPos = currPosition; 
			val readLen = (int) (nextPos - currPosition);
			// *** async read ***
			val blockContentFuture = executorService.submit(
					() -> retryReadAt(storage, filePath, readPos, readLen, inputIOCounter));
			
			res.add(blockContentFuture);
			currPosition = nextPos;
		}
		return res;
	}

	public static byte[] retryReadAt(
			// byte[] resBuffer, int resPos, //
			BlobStorage storage, String filePath, //
			long position, int readLen, //
			BlobStorageIOTimeCounter inputIOCounter) {
		byte[] res = null;
		for(int retry = 0; retry < maxRetryReadRange; retry++) {
			try {
				val startTime = System.currentTimeMillis();
				
				res = storage.readAt(filePath, position, readLen);
				
				val millis = System.currentTimeMillis() - startTime;
				inputIOCounter.incr(BlobStorageIOTimeResult.ofIoRead1(millis, readLen));
				break;
			} catch(Exception ex) {
				val errorMsg = "Failed read " + filePath + " at(" + position + "," + readLen + ")";
				if (retry + 1 < maxRetryReadRange) {
					log.warn(errorMsg + " .. retry [" + retry + "/" + maxRetryReadRange + "] ex:" + ex.getMessage());
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
					}
				} else {
					throw new RuntimeException(errorMsg, ex);
				}
			}
		}
		return res;
	}


	public static void writeFileByBlockFutures(
			BlobStoragePath destStoragePath,
			BlobStorageIOTimeCounter outputIOCounter,
			List<Future<byte[]>> blockContentFutures) {
		try (val output = destStoragePath.openWrite()) {
			// loop wait futures (in order), append to output 
			for(val blockContentFuture: blockContentFutures) {
				byte[] blockContent;
				try {
					blockContent = blockContentFuture.get();
				} catch (InterruptedException ex) {
					throw new RuntimeException("Interrupted.. ", ex);
				} catch (ExecutionException ex) {
					throw new RuntimeException("Failed to write " + destStoragePath + " : fail to read block", ex);
				}
				
				BlobStorageIOUtils.copy(blockContent, output, outputIOCounter);
			}
			
		} catch(IOException ex) {
			throw new RuntimeException("Failed to write " + destStoragePath, ex);
		}
	}
}
