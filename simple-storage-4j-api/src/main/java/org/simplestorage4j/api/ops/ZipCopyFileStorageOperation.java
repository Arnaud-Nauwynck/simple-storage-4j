package org.simplestorage4j.api.ops;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.counter.BlobStorageIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.SrcStorageZipEntryDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.ZipCopyFileStorageOperationDTO;
import org.simplestorage4j.api.util.BlobStorageIOUtils;
import org.simplestorage4j.api.util.BlobStorageUtils;

import com.google.common.collect.ImmutableList;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class ZipCopyFileStorageOperation extends BlobStorageOperation {

    private static final long MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024;
    private static final int DEFAULT_ZIP_BUFFER_SIZE = 30 * 1024 * 1024;

	public final BlobStoragePath destStoragePath;

	public final @Nonnull BlobStorage srcStorage;
    public final ImmutableList<SrcStorageZipEntry> srcEntries;

    public final long totalEntriesFileSize; // = computed from srcEntries srcEntry.srcFileLen

	/**
	 *
	 */
	public static class SrcStorageZipEntry {

		public final @Nonnull String destEntryPath;
		public final @Nonnull String srcStoragePath;
		public final long srcFileLen;
		
		public SrcStorageZipEntry(@Nonnull String destEntryPath,
				@Nonnull String srcStoragePath,
				long srcFileLen) {
			this.destEntryPath = Objects.requireNonNull(destEntryPath);
			this.srcStoragePath = Objects.requireNonNull(srcStoragePath);
			this.srcFileLen = srcFileLen;
		}
		
		public SrcStorageZipEntryDTO toDTO() {
			return new SrcStorageZipEntryDTO(destEntryPath, srcStoragePath, srcFileLen);
		}

		@Override
		public String toString() {
			return "{zip-entry " //
					+ " destEntryPath:" + destEntryPath + "'" //
					+ ", src: '" + srcStoragePath + "'"
					+ " (len: " + srcFileLen + ")"
					+ "}";
		}

	}

    // ------------------------------------------------------------------------
	
    public ZipCopyFileStorageOperation(long jobId, long taskId, //
    		@Nonnull BlobStoragePath destStoragePath,
    		@Nonnull BlobStorage srcStorage,
    		@Nonnull ImmutableList<SrcStorageZipEntry> srcEntries) {
        super(jobId, taskId);
        this.destStoragePath = Objects.requireNonNull(destStoragePath);
        this.srcStorage = srcStorage;
        this.srcEntries = Objects.requireNonNull(srcEntries);
        long totalEntriesFileSize = 0;
        for(val srcEntry: srcEntries) {
            totalEntriesFileSize += srcEntry.srcFileLen;
        }
        this.totalEntriesFileSize = totalEntriesFileSize;

    }

    // ------------------------------------------------------------------------

    @Override
    public String taskTypeName() {
        return "zip-copy-file";
    }

    @Override
	public PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost() {
    	val srcIOCost = BlobStoragePreEstimateIOCost.ofIoRead(totalEntriesFileSize, srcEntries.size());
    	val destIOCost = BlobStoragePreEstimateIOCost.ofIoWrite(totalEntriesFileSize, 1);
    	return PerBlobStoragesPreEstimateIOCost.of(srcStorage, srcIOCost,
    			destStoragePath.blobStorage, destIOCost);
	}

	@Override
	public BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx) {
		val startTime = System.currentTimeMillis();
		val inputIOCounter = new BlobStorageIOTimeCounter();
		val outputIOCounter = new BlobStorageIOTimeCounter();

		try (val output = destStoragePath.openWrite()) {
			val zipOutput = new ZipOutputStream(new BufferedOutputStream(output, DEFAULT_ZIP_BUFFER_SIZE));

			for(val srcEntry: srcEntries) {
				ZipEntry zipEntry = new ZipEntry(srcEntry.destEntryPath);
				zipOutput.putNextEntry(zipEntry);

				if (srcEntry.srcFileLen < MAX_DOWNLOAD_BYTES) {
					// for small file, download content with retry..
					val startReadTime = System.currentTimeMillis();

					byte[] data = readSrcFileWithRetry(srcEntry);

					val readMillis = System.currentTimeMillis() - startReadTime;
					inputIOCounter.incr(readMillis, data.length, 0L, 1, 0, 0);

					// TOADD check data.length == srcFileLen

					zipOutput.write(data);

					val writeMillis = System.currentTimeMillis() - readMillis;
					outputIOCounter.incr(writeMillis, 0L, data.length, 1, 0, 0);

				} else {
					// for big file, download to temporary file? or stream in to out directly (BUT no retry on error???)

					// TODO too many errors ... need retry !!!
					// use temporary local file? or split by ranges

					try (val entryInput = srcStorage.openRead(srcEntry.srcStoragePath)) {
						// equivalent to .. IOUtils.copy(input, output);
						// with IOstats per input|output
						BlobStorageIOUtils.copy(entryInput, inputIOCounter, output, outputIOCounter);
					}
				}

				zipOutput.closeEntry();

			}

			zipOutput.flush();

		} catch(IOException ex) {
			throw new RuntimeException("Failed " + toString(), ex);
		}

		val millis = System.currentTimeMillis();
		val res = BlobStorageOperationResult.of(jobId, taskId, startTime, millis,
				srcStorage.id, inputIOCounter.toImmutable(),
				destStoragePath.blobStorage.id, outputIOCounter.toImmutable()
				);
		ctx.logIncr_zipCopyFile(this, res, logPrefix -> log.info(logPrefix + "(" + destStoragePath + ", srcEntries.count:" + srcEntries.size() + ", totalEntriesFileSize:" + totalEntriesFileSize + ")"));
		return res;
	}

	private byte[] readSrcFileWithRetry(SrcStorageZipEntry srcEntry) {
		byte[] res = null;
		int maxRetry = 5;
		for(int retry = 0; retry < maxRetry; retry++) {
			try {
				res = srcStorage.readFile(srcEntry.srcStoragePath);
				break;
			} catch(RuntimeException ex) {
				if (retry + 1 < maxRetry) {
					log.warn("Failed read file '" + srcEntry.srcStoragePath + "' ..retry " + ex.getMessage());
					continue;
				} else {
					throw ex;
				}
			}
		}
		return res;
	}


	@Override
    public BlobStorageOperationDTO toDTO() {
    	val entryDtos = BlobStorageUtils.map(srcEntries, x -> x.toDTO());
		return new ZipCopyFileStorageOperationDTO(jobId, taskId, destStoragePath.toDTO(), srcStorage.id.id, entryDtos);
    }

	@Override
	public String toString() {
		return "{zip-copy-file " //
				+ " dest:" + destStoragePath //
				+ " totalEntriesFileSize: " + totalEntriesFileSize
				+ ", srcEntries=" + srcEntries
				+ "}";
	}
	
}
