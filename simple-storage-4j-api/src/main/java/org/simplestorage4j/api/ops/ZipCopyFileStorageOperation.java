package org.simplestorage4j.api.ops;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.SrcStorageZipEntryDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.ZipCopyFileStorageOperationDTO;
import org.simplestorage4j.api.util.BlobStorageNotImpl;
import org.simplestorage4j.api.util.BlobStorageUtils;

import com.google.common.collect.ImmutableList;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class ZipCopyFileStorageOperation extends BlobStorageOperation {

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
		BlobStorageOperationResult res = null;
		// TODO
		ctx.logIncr_zipCopyFile(this, res , logPrefix -> log.info(logPrefix + "(" + destStoragePath + ", srcEntries.count:" + srcEntries.size() + ", totalEntriesFileSize:" + totalEntriesFileSize + ")"));
		throw BlobStorageNotImpl.notImpl();
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
