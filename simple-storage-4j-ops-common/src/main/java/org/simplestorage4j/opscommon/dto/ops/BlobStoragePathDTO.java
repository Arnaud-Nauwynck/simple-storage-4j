package org.simplestorage4j.opscommon.dto.ops;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO for BlobStoragePath
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class BlobStoragePathDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String storageId;
	public String path;

}
