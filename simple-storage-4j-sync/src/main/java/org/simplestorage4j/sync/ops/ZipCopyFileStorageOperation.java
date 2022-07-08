package org.simplestorage4j.sync.ops;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.sync.ops.stats.BlobStorageOperationCost;
import org.simplestorage4j.sync.util.BlobStorageNotImpl;

import com.google.common.collect.ImmutableList;

/**
 * 
 */
public class ZipCopyFileStorageOperation extends BlobStorageOperation {

	public final BlobStoragePath destStoragePath;

	public final @Nonnull BlobStorage srcStorage;
    public final ImmutableList<SrcStorageZipEntry> srcEntries;

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
    }

    // ------------------------------------------------------------------------
    
    @Override
    public String taskTypeName() {
        return "zip-copy-file";
    }

    @Override
	public BlobStorageOperationCost estimateExecutionCost() {
		// TODO
		throw BlobStorageNotImpl.notImpl();
	}

	@Override
	public BlobStorageOperationExecutionResult execute() {
		// TODO
		throw BlobStorageNotImpl.notImpl();
	}

	@Override
	public String toString() {
		return "{zip-copy-file " // 
				+ " dest:" + destStoragePath // 
				+ ", srcEntries=" + srcEntries 
				+ "}";
	}
	
}
