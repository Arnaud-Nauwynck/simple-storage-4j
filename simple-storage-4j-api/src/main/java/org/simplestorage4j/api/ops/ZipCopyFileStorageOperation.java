package org.simplestorage4j.api.ops;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.util.BlobStorageNotImpl;

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
	
    public ZipCopyFileStorageOperation(int taskId, //
    		@Nonnull BlobStoragePath destStoragePath,
    		@Nonnull BlobStorage srcStorage,
    		@Nonnull ImmutableList<SrcStorageZipEntry> srcEntries) {
        super(taskId);
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
	public PerBlobStoragesIOTimeResult execute(BlobStorageOperationExecContext ctx) {
		PerBlobStoragesIOTimeResult res = null;
		// TODO
		ctx.logIncr_zipCopyFile(this, res , logPrefix -> log.info(logPrefix + "(" + destStoragePath + ", srcEntries.count:" + srcEntries.size() + ", totalEntriesFileSize:" + totalEntriesFileSize + ")"));
		throw BlobStorageNotImpl.notImpl();
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
