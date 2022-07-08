package org.simplestorage4j.sync.ops.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class BlobStorageIOCostDTO implements Serializable {

	/** for java.io.Serializable */
	private static final long serialVersionUID = 1L;
	
	// private long timeMillis; // computable from BlobStorageSpeedAverage
	public long ioReadLen;
	public long ioWriteLen;
	public int callCount;
	public int metadataReadCount;
	public int metadataWriteCount;

}
