package org.simplestorage4j.api.ops.dto;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.CopyFileContentStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.CopyFileStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.MkdirStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.ZipCopyFileStorageOperationDTO;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * DTO class for {@link org.simplestorage4j.api.ops.BlobStorageOperation}
 *
 */
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
	@Type(name="copy-file-content", value=CopyFileContentStorageOperationDTO.class),
	@Type(name="copy-file", value=CopyFileStorageOperationDTO.class),
	@Type(name="mkdir", value=MkdirStorageOperationDTO.class),
	@Type(name="zip-copy-file", value=ZipCopyFileStorageOperationDTO.class),
})
@NoArgsConstructor @AllArgsConstructor
public abstract class BlobStorageOperationDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	// public abstract String getType();

	public long jobId;
	public long taskId;

	// ------------------------------------------------------------------------

	@NoArgsConstructor
	public static class CopyFileContentStorageOperationDTO extends BlobStorageOperationDTO {

		/** */
		private static final long serialVersionUID = 1L;

		public @Nonnull BlobStoragePathDTO destStoragePath;
		public @Nonnull byte[] srcContent;

		public CopyFileContentStorageOperationDTO(long jobId, long taskId, //
				BlobStoragePathDTO destStoragePath, byte[] srcContent) {
			super(jobId, taskId);
			this.destStoragePath = destStoragePath;
			this.srcContent = srcContent;
		}


	}

	// ------------------------------------------------------------------------

	@NoArgsConstructor
	public static class CopyFileStorageOperationDTO extends BlobStorageOperationDTO {

		/** */
		private static final long serialVersionUID = 1L;

		public @Nonnull BlobStoragePathDTO destStoragePath;
		public @Nonnull BlobStoragePathDTO srcStoragePath;
		public long srcFileLen;

		public CopyFileStorageOperationDTO(long jobId, long taskId, //
				BlobStoragePathDTO destStoragePath, BlobStoragePathDTO srcStoragePath,
				long srcFileLen) {
			super(jobId, taskId);
			this.destStoragePath = destStoragePath;
			this.srcStoragePath = srcStoragePath;
			this.srcFileLen = srcFileLen;
		}

	}

	// ------------------------------------------------------------------------

	public static class MkdirStorageOperationDTO extends BlobStorageOperationDTO {

		/** */
		private static final long serialVersionUID = 1L;

		public @Nonnull BlobStoragePathDTO storagePath;

		public MkdirStorageOperationDTO(long jobId, long taskId, //
				BlobStoragePathDTO storagePath) {
			super(jobId, taskId);
			this.storagePath = storagePath;
		}

	}

	// ------------------------------------------------------------------------

	public static class ZipCopyFileStorageOperationDTO extends BlobStorageOperationDTO {

		/** */
		private static final long serialVersionUID = 1L;

		public @Nonnull BlobStoragePathDTO destStoragePath;
		public @Nonnull String srcStorageId;
	    public List<SrcStorageZipEntryDTO> srcEntries;
	    // public long totalEntriesFileSize; // = computed from srcEntries srcEntry.srcFileLen

	    public ZipCopyFileStorageOperationDTO(long jobId, long taskId, //
	    		BlobStoragePathDTO destStoragePath, String srcStorageId,
	    		List<SrcStorageZipEntryDTO> srcEntries) {
	    	super(jobId, taskId);
	    	this.destStoragePath = destStoragePath;
	    	this.srcStorageId = srcStorageId;
	    	this.srcEntries = srcEntries;
	    }
	
	}
	
	@AllArgsConstructor @NoArgsConstructor
	public static class SrcStorageZipEntryDTO implements Serializable {

		/** */
		private static final long serialVersionUID = 1L;

		public @Nonnull String destEntryPath;
		public @Nonnull String srcStoragePath;
		public long srcFileLen;

		@Override
		public String toString() {
			return "{zip-entry " //
					+ " destEntryPath:" + destEntryPath + "'" //
					+ ", src: '" + srcStoragePath + "'"
					+ " (len: " + srcFileLen + ")"
					+ "}";
		}
	}

}
