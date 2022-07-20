package org.simplestorage4j.api.ops.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationErrorDTO;
import org.simplestorage4j.api.util.BlobStorageUtils;

import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class BlobStorageOperationError {
	
	public final BlobStorageOperation op;
	
	public final List<BlobStorageOperationResult> results = new ArrayList<>();
	
	public Throwable unexpectedError;
	
	// ------------------------------------------------------------------------
	
	public void addError(BlobStorageOperationResult result) {
		this.results.add(result);
	}

	public BlobStorageOperationErrorDTO toDTO() {
		val opDto = op.toDTO();
		val resultDtos = BlobStorageOperationResult.toDtos(results);
		val unexpectedErrorMessage = (unexpectedError != null)? unexpectedError.getMessage() : null;
		return new BlobStorageOperationErrorDTO(opDto, resultDtos, unexpectedErrorMessage);
	}

	public static List<BlobStorageOperationErrorDTO> toDTOs(Collection<BlobStorageOperationError> src) {
		return BlobStorageUtils.map(src, x -> x.toDTO());
	}

}