package org.simplestorage4j.sync.ops;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;

import com.google.common.collect.ImmutableList;

/**
 * 
 */
public class ZipCopyStorageFileOperation extends BlobStorageOperation {

	public final BlobStorage destStorage;
	public final String destPath;

    public final ImmutableList<SrcStorageZipEntry> srcEntries;

	/**
	 * 
	 */
	public static class SrcStorageZipEntry {

		public final @Nonnull String destEntryPath;
		public final @Nonnull BlobStorage srcStorage;
		public final @Nonnull String srcPath;
		
		public SrcStorageZipEntry(@Nonnull String destEntryPath, 
				@Nonnull BlobStorage srcStorage, 
				@Nonnull String srcPath) {
			this.destEntryPath = Objects.requireNonNull(destEntryPath);
			this.srcStorage = Objects.requireNonNull(srcStorage);
			this.srcPath = Objects.requireNonNull(srcPath);
		}
		
		@Override
		public String toString() {
			return "{zip-entry " // 
					+ " destEntryPath:" + destEntryPath + "'" // 
					+ ", src: " + srcStorage.displayName + " '" + srcPath + "'" 
					+ "]";
		}

	}

    // ------------------------------------------------------------------------
	
    public ZipCopyStorageFileOperation(int taskId, //
    		BlobStorage destStorage,
    		String destPath, 
    		ImmutableList<SrcStorageZipEntry> srcEntries) {
        super(taskId);
        this.destStorage = destStorage;
        this.destPath = destPath;
        this.srcEntries = srcEntries;
    }

    // ------------------------------------------------------------------------
    
    @Override
    public String taskTypeName() {
        return "zip-copy-file";
    }

    @Override
	public BlobStorageOperationExecutionCostEstimation estimateExecutionCost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BlobStorageOperationExecutionResult execute() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toString() {
		return "{zip-copy-file " // 
				+ " dest:" + destStorage.displayName + " '" + destPath + "'" // 
				+ ", srcEntries=" + srcEntries 
				+ "}";
	}

	
}