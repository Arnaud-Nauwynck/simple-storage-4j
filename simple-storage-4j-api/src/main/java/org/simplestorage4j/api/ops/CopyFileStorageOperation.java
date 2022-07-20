package org.simplestorage4j.api.ops;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.counter.BlobStorageIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.CopyFileStorageOperationDTO;
import org.simplestorage4j.api.util.BlobStorageIOUtils;

import lombok.val;

/**
 *
 */
public class CopyFileStorageOperation extends BlobStorageOperation {

	public static final long MAX_CONTENT_BYTES = 10 * 1024 * 1024;

	public final @Nonnull BlobStoragePath destStoragePath;

	public final @Nonnull BlobStoragePath srcStoragePath;
	public final long srcFileLen;

	// ------------------------------------------------------------------------

	public CopyFileStorageOperation(long jobId, long taskId, //
			@Nonnull BlobStoragePath destStoragePath, //
			@Nonnull BlobStoragePath srcStoragePath, //
			long srcFileLen) {
		super(jobId, taskId);
		this.destStoragePath = Objects.requireNonNull(destStoragePath);
		this.srcStoragePath = Objects.requireNonNull(srcStoragePath);
		this.srcFileLen = srcFileLen;
	}

	// ------------------------------------------------------------------------

	@Override
	public String taskTypeName() {
		return "copy-file";
	}

	@Override
	public PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost() {
		return PerBlobStoragesPreEstimateIOCost.of( //
				srcStoragePath.blobStorage, BlobStoragePreEstimateIOCost.ofIoRead1(srcFileLen),
				destStoragePath.blobStorage, BlobStoragePreEstimateIOCost.ofIoWrite1(srcFileLen));
	}

	@Override
	public BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx) {
		val startTime = System.currentTimeMillis();
		val inputIOCounter = new BlobStorageIOTimeCounter();
		val outputIOCounter = new BlobStorageIOTimeCounter();

		if (this.srcFileLen < MAX_CONTENT_BYTES) {
			val startReadTime = System.currentTimeMillis();

			byte[] data = srcStoragePath.readFile();

			val readMillis = System.currentTimeMillis() - startReadTime;
			inputIOCounter.incr(readMillis, data.length, 0L, 1, 0, 0);

			// TOADD check data.length == srcFileLen

			destStoragePath.writeFile(data);

			val writeMillis = System.currentTimeMillis() - readMillis;
			outputIOCounter.incr(writeMillis, 0L, data.length, 1, 0, 0);

		} else {
			try (val input = srcStoragePath.openRead()) {
				try (val output = destStoragePath.openWrite()) {
					// equivalent to .. IOUtils.copy(input, output);
					// with IOstats per input|output
					BlobStorageIOUtils.copy(input, inputIOCounter, output, outputIOCounter);
				}
			} catch(IOException ex) {
				throw new RuntimeException("Failed " + toString(), ex);
			}
		}

		val millis = System.currentTimeMillis();
		return BlobStorageOperationResult.of(jobId, taskId, startTime, millis,
				srcStoragePath.blobStorage.id, inputIOCounter.toImmutable(),
				destStoragePath.blobStorage.id, outputIOCounter.toImmutable()
				);
	}

	@Override
	public BlobStorageOperationDTO toDTO() {
		return new CopyFileStorageOperationDTO(jobId, taskId, destStoragePath.toDTO(), srcStoragePath.toDTO(), srcFileLen);
	}

	@Override
	public String toString() {
		return "{copy-file " + taskId //
				+ " dest: " + destStoragePath + " src: " + srcStoragePath + " (len: " + srcFileLen + ")" //
				+ "}";
	}

}
