package org.simplestorage4j.api.ops;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

import lombok.AllArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * operation to zip several files from src storage, and copy to dest storage 
 */
@Slf4j
public class ZipCopyFileStorageOperation extends BlobStorageOperation {

	private static final int EXTRA_ZIP_BUFFER_SIZE = 32 * 1024;
	private static final int MAX_ZIP_BUFFER_SIZE = 10 * 1024 * 1024;
	private static final long defaultReadContentMaxLen = 10 * 1024 * 1024;

	public final BlobStoragePath destStoragePath;

	public final @Nonnull BlobStorage srcStorage;
	public final ImmutableList<SrcStorageZipEntry> srcEntries;

	public final long totalEntriesFileSize; // = computed from srcEntries srcEntry.srcFileLen

	/**
	 * zip entry of zip-copy-file operation 
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

	@AllArgsConstructor
	public static class ZipEntryContent {
		final SrcStorageZipEntry zipEntry;
		final List<Future<byte[]>> srcContentBlockFutures;
	}

	@Override
	public BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx) {
		val startTime = System.currentTimeMillis();
		val inputIOCounter = new BlobStorageIOTimeCounter();
		val outputIOCounter = new BlobStorageIOTimeCounter();

		val zipEntryContentFutures = new ArrayList<Future<ZipEntryContent>>();
		for(val srcEntry: srcEntries) {
			// *** async read ***
			val zipEntryContentFuture = ctx.submitSubTask(() -> readSrcEntry(ctx, srcEntry, inputIOCounter));
			zipEntryContentFutures.add(zipEntryContentFuture);
		}

		try (val output = destStoragePath.openWrite()) {
			val bufferSize = (int) Math.min(totalEntriesFileSize + EXTRA_ZIP_BUFFER_SIZE, MAX_ZIP_BUFFER_SIZE);
			val zipOutput = new ZipOutputStream(new BufferedOutputStream(output, bufferSize));

			// *** loop read futures (in order), write to zip ***
			for(val zipEntryContentFuture : zipEntryContentFutures) {
				ZipEntryContent zipEntryContent;
				try {
					zipEntryContent = zipEntryContentFuture.get();
				} catch (InterruptedException | ExecutionException ex) {
					throw new RuntimeException("Failed " + toString() + ": failed to get entry content ", ex);
				}

				val srcZipEntry = zipEntryContent.zipEntry;
				val srcContentBlockFutures = zipEntryContent.srcContentBlockFutures;

				ZipEntry zipEntry = new ZipEntry(srcZipEntry.destEntryPath);
				zipOutput.putNextEntry(zipEntry);

				for(val srcContentBlockFuture : srcContentBlockFutures) {
					byte[] srcContentBlock;
					try {
						srcContentBlock = srcContentBlockFuture.get();
					} catch (InterruptedException | ExecutionException ex) {
						throw new RuntimeException("Failed " + toString() + ": failed to get entry " + srcZipEntry.destEntryPath + " block content ", ex);
					}

					val startWrite = System.currentTimeMillis();

					zipOutput.write(srcContentBlock);

					val writeMillis = System.currentTimeMillis() - startWrite;
					outputIOCounter.incr(writeMillis, 0L, srcContentBlock.length, 1, 0, 0);
				}

				zipOutput.closeEntry();

			}

			val flushStartTime = System.currentTimeMillis();

			// zipOutput.finish();
			zipOutput.flush();

			val flushMillis = System.currentTimeMillis() - flushStartTime;
			outputIOCounter.incr(flushMillis, 0L, 0L, 1, 0, 0);

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

	private ZipEntryContent readSrcEntry(BlobStorageOperationExecContext ctx, 
			SrcStorageZipEntry srcEntry, BlobStorageIOTimeCounter inputIOCounter) {
		List<Future<byte[]>> srcContentBlockFutures;
		if (srcEntry.srcFileLen < defaultReadContentMaxLen) {
			// for small file, download content fully(by streaming) with retry..
			byte[] content = BlobStorageIOUtils.readFileWithRetry(srcStorage, srcEntry.srcStoragePath, inputIOCounter);
			srcContentBlockFutures = new ArrayList<>();
			srcContentBlockFutures.add(CompletableFuture.completedFuture(content));
		} else {
			// for big file, async read by ranges (with retry per range)... return ordered future list 
			srcContentBlockFutures = BlobStorageIOUtils.asyncReadFileByBlocksWithRetry(
					srcStorage, srcEntry.srcStoragePath, srcEntry.srcFileLen, inputIOCounter, ctx.getSubTasksExecutor());
		}
		return new ZipEntryContent(srcEntry, srcContentBlockFutures);
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
