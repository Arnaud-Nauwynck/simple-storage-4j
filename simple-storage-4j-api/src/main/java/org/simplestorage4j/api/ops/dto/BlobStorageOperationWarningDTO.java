package org.simplestorage4j.api.ops.dto;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class BlobStorageOperationWarningDTO implements Serializable {
	
	/** */
	private static final long serialVersionUID = 1L;

	public BlobStorageOperationDTO op;
	
	public List<String> warnings;
	
}
