package org.simplestorage4j.api.ops;

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
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class CopyFileStorageOperation extends BlobStorageOperation {

	public static final long defaultReadContentMaxLen = 10 * 1024 * 1024;
	private static boolean useReadByStreaming = false; // for debug only

	
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

		if (this.srcFileLen < defaultReadContentMaxLen) {
			byte[] data = BlobStorageIOUtils.readFileWithRetry(srcStoragePath, inputIOCounter);
			// TOADD check data.length == srcFileLen
			BlobStorageIOUtils.writeFile(destStoragePath, data, outputIOCounter);
		} else {
			if (useReadByStreaming) {
				BlobStorageIOUtils.copyFileUsingStreaming(srcStoragePath, inputIOCounter, destStoragePath, outputIOCounter);
			} else {
				// using read by range futures
				val blockContentFutures = BlobStorageIOUtils.asyncReadFileByBlocksWithRetry(
						srcStoragePath, srcFileLen, inputIOCounter, ctx.getSubTasksExecutor());
				BlobStorageIOUtils.writeFileByBlockFutures(destStoragePath, outputIOCounter, blockContentFutures);
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
