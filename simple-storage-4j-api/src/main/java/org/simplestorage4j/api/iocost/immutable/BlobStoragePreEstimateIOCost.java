package org.simplestorage4j.api.iocost.immutable;

import java.util.LinkedHashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.BlobStoragePreEstimateIOCostDTO;

import com.google.common.collect.ImmutableMap;

import lombok.val;

public class BlobStoragePreEstimateIOCost {
	
	// private long timeMillis; // computable from BlobStorageSpeedAverage
	public final long ioReadLen;
	public final long ioWriteLen;
	public final int callCount;
	public final int metadataReadCount;
	public final int metadataWriteCount;
	
	// ------------------------------------------------------------------------
	
	public BlobStoragePreEstimateIOCost(long ioReadLen, long ioWriteLen, // 
			int callCount, int metadataReadCount, int metadataWriteCount) {
		this.ioReadLen = ioReadLen;
		this.ioWriteLen = ioWriteLen;
		this.callCount = callCount;
		this.metadataReadCount = metadataReadCount;
		this.metadataWriteCount = metadataWriteCount;
	}
	
	public static BlobStoragePreEstimateIOCost of(long ioReadLen, long ioWriteLen, // 
			int callCount, int metadataReadCount, int metadataWriteCount) {
		return new BlobStoragePreEstimateIOCost(ioReadLen, ioWriteLen, // 
				callCount, metadataReadCount, metadataWriteCount);
	}
	
	public static BlobStoragePreEstimateIOCost ofMetadataCall(int callCount, int metadataReadCount, int metadataWriteCount) {
		return of(0L, 0L, callCount, metadataReadCount, metadataWriteCount);
	}

	public static BlobStoragePreEstimateIOCost ofIoRead(long ioReadLen, int callCount) {
		return of(ioReadLen, 0L, callCount, 0, 0);
	}

	public static BlobStoragePreEstimateIOCost ofIoWrite(long ioWriteLen, int callCount) {
		return of(0L, ioWriteLen, callCount, 0, 0);
	}

	public static BlobStoragePreEstimateIOCost ofIoRead1(long ioReadLen) {
		return ofIoRead(ioReadLen, 1);
	}

	public static BlobStoragePreEstimateIOCost ofIoWrite1(long ioWriteLen) {
		return ofIoWrite(ioWriteLen, 1);
	}
	
	public static BlobStoragePreEstimateIOCost fromDTO(BlobStoragePreEstimateIOCostDTO src) {
		return new BlobStoragePreEstimateIOCost(src.ioReadLen, src.ioWriteLen, src.callCount, src.metadataReadCount, src.metadataWriteCount);
	}
	
	public static ImmutableMap<BlobStorageId,BlobStoragePreEstimateIOCost> fromDTOsMap(Map<String,BlobStoragePreEstimateIOCostDTO> srcs) {
		val res = ImmutableMap.<BlobStorageId,BlobStoragePreEstimateIOCost>builder();
		if (srcs != null) {
			for(val e : srcs.entrySet()) {
				val id = new BlobStorageId(e.getKey());
				val value = BlobStoragePreEstimateIOCost.fromDTO(e.getValue());
				res.put(id, value);
			}
		}
		return res.build();
	}
	
	// ------------------------------------------------------------------------
	
	public BlobStoragePreEstimateIOCostDTO toDTO() {
		return new BlobStoragePreEstimateIOCostDTO(ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount);
	}

	public static Map<String,BlobStoragePreEstimateIOCostDTO> toDTOsMap(Map<BlobStorageId,BlobStoragePreEstimateIOCost> srcs) {
		val res = new LinkedHashMap<String,BlobStoragePreEstimateIOCostDTO>();
		for(val e: srcs.entrySet()) {
			val id = e.getKey().id;
			val value = e.getValue().toDTO();
			res.put(id, value);
		}
		return res;
	}
	
	@Override
	public String toString() {
		return "{PreEstimateIOCost " //
				+ ((ioReadLen != 0)? "ioRead=" + ioReadLen : "") //
				+ ((ioWriteLen != 0)? " ioWrite=" + ioWriteLen : "") //
				+ ((callCount > 1)? " call=" + callCount : "") //
				+ ((metadataReadCount != 0)? " metadataRead=" + metadataReadCount : "") // 
				+ ((metadataWriteCount != 0)? " metadataWrite=" + metadataWriteCount : "") //
				+ "}";
	}

}
