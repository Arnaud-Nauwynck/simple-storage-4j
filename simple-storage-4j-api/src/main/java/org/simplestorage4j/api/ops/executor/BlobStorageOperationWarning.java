package org.simplestorage4j.api.ops.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationWarningDTO;
import org.simplestorage4j.api.util.BlobStorageUtils;

import com.google.common.collect.ImmutableList;

import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class BlobStorageOperationWarning {
	
	public final BlobStorageOperation op;
	
	public final ImmutableList<String> warnings;
	
	// ------------------------------------------------------------------------
	
	public BlobStorageOperationWarningDTO toDTO() {
		val opDto = op.toDTO();
		return new BlobStorageOperationWarningDTO(opDto, new ArrayList<>(warnings));
	}

	public static List<BlobStorageOperationWarningDTO> toDTOs(Collection<BlobStorageOperationWarning> src) {
		return BlobStorageUtils.map(src, x -> x.toDTO());
	}

}