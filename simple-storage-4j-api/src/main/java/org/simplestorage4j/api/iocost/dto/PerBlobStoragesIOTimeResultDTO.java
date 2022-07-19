package org.simplestorage4j.api.iocost.dto;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO class for BlobStorage operation execution result
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class PerBlobStoragesIOTimeResultDTO implements Serializable {

	/** for java.io.Serializable */
	private static final long serialVersionUID = 1L;

	public Map<String,BlobStorageIOTimeResultDTO> perStorageIOCosts = new LinkedHashMap<>();

	@Override
	public String toString() {
		return "{" + perStorageIOCosts + "}";
	}

}
