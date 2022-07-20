package org.simplestorage4j.api.ops.dto;

import java.io.Serializable;
import java.util.List;

import org.simplestorage4j.api.iocost.dto.BlobStorageOperationResultDTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class BlobStorageOperationErrorDTO implements Serializable {
	
	/** */
	private static final long serialVersionUID = 1L;

	public BlobStorageOperationDTO op;
	
	public List<BlobStorageOperationResultDTO> results;
	
	public String unexpectedErrorMessage;

}
