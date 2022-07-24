package org.simplestorage4j.api.iocost.dto;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
public class PerBlobStoragesPreEstimateIOCostDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;
	
	public Map<String,BlobStoragePreEstimateIOCostDTO> perStorage = new LinkedHashMap<>();

}
