package org.simplestorage4j.sync.ops.dto;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
public class BlobStorageOperationCostDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;
	
	public Map<String,BlobStorageIOCostDTO> perStorage = new LinkedHashMap<>();

}
