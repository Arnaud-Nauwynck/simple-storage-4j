package org.simplestorage4j.api.ops.encoder;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.BlobStorageRepository;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.CopyFileContentStorageOperation;
import org.simplestorage4j.api.ops.CopyFileStorageOperation;
import org.simplestorage4j.api.ops.MkdirStorageOperation;
import org.simplestorage4j.api.ops.ZipCopyFileStorageOperation;
import org.simplestorage4j.api.ops.ZipCopyFileStorageOperation.SrcStorageZipEntry;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.CopyFileContentStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.CopyFileStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.MkdirStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.ZipCopyFileStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStoragePathDTO;
import org.simplestorage4j.api.util.BlobStorageUtils;

import com.google.common.collect.ImmutableList;

import lombok.val;

public class BlobStorageOperationDtoResolver {

	private final BlobStorageRepository blobStorageRepository;
	
	// ------------------------------------------------------------------------
	
	public BlobStorageOperationDtoResolver(BlobStorageRepository blobStorageRepository) {
		this.blobStorageRepository = blobStorageRepository;
	}

	// ------------------------------------------------------------------------
	
	public List<BlobStorageOperation> dtosToOps(Collection<BlobStorageOperationDTO> dtos) {
		return BlobStorageUtils.map(dtos, x -> dtoToOp(x));
	}

	public BlobStorageOperation dtoToOp(BlobStorageOperationDTO dto) {
		BlobStorageOperation res = null;
		if (dto == null) {
			return null;
		}
		val jobId = dto.jobId;
		val taskId = dto.taskId;
		if (dto instanceof CopyFileContentStorageOperationDTO) {
			val src = (CopyFileContentStorageOperationDTO) dto;
			val destStoragePath = toBlobStoragePath(src.destStoragePath);
    		byte[] srcContent = Objects.requireNonNull(src.srcContent); 
			res = new CopyFileContentStorageOperation(jobId, taskId, destStoragePath, srcContent);
		} else if (dto instanceof CopyFileStorageOperationDTO) {
			val src = (CopyFileStorageOperationDTO) dto;
			val destStoragePath = toBlobStoragePath(src.destStoragePath);
			val srcStoragePath = toBlobStoragePath(src.srcStoragePath);
			long srcFileLen = src.srcFileLen;
			res = new CopyFileStorageOperation(jobId, taskId, destStoragePath, srcStoragePath, srcFileLen);
		} else if (dto instanceof MkdirStorageOperationDTO) {
			val src = (MkdirStorageOperationDTO) dto;
			val storagePath = toBlobStoragePath(src.storagePath);
			res = new MkdirStorageOperation(jobId, taskId, storagePath);
		} else if (dto instanceof ZipCopyFileStorageOperationDTO) {
			val src = (ZipCopyFileStorageOperationDTO) dto;
			val destStoragePath = toBlobStoragePath(src.destStoragePath);
			val srcStorage = toBlobStorage(src.srcStorageId);
			val zipEntries = ImmutableList.<SrcStorageZipEntry>builder();
			for(val srcEntry: src.srcEntries) {
				val zipEntry = new SrcStorageZipEntry(srcEntry.destEntryPath, srcEntry.srcStoragePath, srcEntry.srcFileLen);
				zipEntries.add(zipEntry);
			}
			res = new ZipCopyFileStorageOperation(jobId, taskId, destStoragePath, srcStorage, zipEntries.build());
		} else {
			throw new IllegalStateException("should not occur: unhandled dto class " + dto.getClass());
		}
		return res;
	}

	private BlobStorage toBlobStorage(String storageId) {
		return blobStorageRepository.get(new BlobStorageId(storageId));
	}

	private BlobStoragePath toBlobStoragePath(BlobStoragePathDTO src) {
		BlobStorage blobStorage = toBlobStorage(src.storageId);
		String path = Objects.requireNonNull(src.path);
		return new BlobStoragePath(blobStorage, path);
	}

}
