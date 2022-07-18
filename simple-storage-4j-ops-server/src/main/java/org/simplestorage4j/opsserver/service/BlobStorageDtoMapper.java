package org.simplestorage4j.opsserver.service;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.simplestorage4j.api.BlobStorageRepository;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.opscommon.dto.ops.BlobStorageOperationDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class BlobStorageDtoMapper {

	@Autowired
	private BlobStorageRepository blobStorageRepository;
	
	// ------------------------------------------------------------------------
	
	public List<BlobStorageOperation> dtosToOps(Collection<BlobStorageOperationDTO> dtos) {
		return dtos.stream().map(x -> dtoToOp(x)).collect(Collectors.toList());
	}

	public BlobStorageOperation dtoToOp(BlobStorageOperationDTO dto) {
		BlobStorageOperation res = null;
		// TODO
		return res;
	}

}
