package org.simplestorage4j.sync.ops.stats;

import java.util.LinkedHashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.sync.ops.dto.BlobStorageIOCostDTO;

import com.google.common.collect.ImmutableMap;

import lombok.val;

public class BlobStorageIOCost {
	
	// private long timeMillis; // computable from BlobStorageSpeedAverage
	public final long ioReadLen;
	public final long ioWriteLen;
	public final int callCount;
	public final int metadataReadCount;
	public final int metadataWriteCount;
	
	// ------------------------------------------------------------------------
	
	public BlobStorageIOCost(long ioReadLen, long ioWriteLen, // 
			int callCount, int metadataReadCount, int metadataWriteCount) {
		this.ioReadLen = ioReadLen;
		this.ioWriteLen = ioWriteLen;
		this.callCount = callCount;
		this.metadataReadCount = metadataReadCount;
		this.metadataWriteCount = metadataWriteCount;
	}
	
	public static BlobStorageIOCost of(long ioReadLen, long ioWriteLen, // 
			int callCount, int metadataReadCount, int metadataWriteCount) {
		return new BlobStorageIOCost(ioReadLen, ioWriteLen, // 
				callCount, metadataReadCount, metadataWriteCount);
	}
	
	public static BlobStorageIOCost ofMetadataCall(int callCount, int metadataReadCount, int metadataWriteCount) {
		return of(0L, 0L, callCount, metadataReadCount, metadataWriteCount);
	}

	public static BlobStorageIOCost ofIoRead(long ioReadLen, int callCount) {
		return of(ioReadLen, 0L, callCount, 0, 0);
	}

	public static BlobStorageIOCost ofIoWrite(long ioWriteLen, int callCount) {
		return of(0L, ioWriteLen, callCount, 0, 0);
	}

	public static BlobStorageIOCost ofIoRead1(long ioReadLen) {
		return ofIoRead(ioReadLen, 1);
	}

	public static BlobStorageIOCost ofIoWrite1(long ioWriteLen) {
		return ofIoWrite(ioWriteLen, 1);
	}
	
	public static BlobStorageIOCost fromDTO(BlobStorageIOCostDTO src) {
		return new BlobStorageIOCost(src.ioReadLen, src.ioWriteLen, src.callCount, src.metadataReadCount, src.metadataWriteCount);
	}
	
	public static ImmutableMap<BlobStorageId,BlobStorageIOCost> fromDTOsMap(Map<String,BlobStorageIOCostDTO> srcs) {
		val res = ImmutableMap.<BlobStorageId,BlobStorageIOCost>builder();
		if (srcs != null) {
			for(val e : srcs.entrySet()) {
				val id = new BlobStorageId(e.getKey());
				val value = BlobStorageIOCost.fromDTO(e.getValue());
				res.put(id, value);
			}
		}
		return res.build();
	}
	
	// ------------------------------------------------------------------------
	
	public BlobStorageIOCostDTO toDTO() {
		return new BlobStorageIOCostDTO(ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount);
	}

	public static Map<String,BlobStorageIOCostDTO> toDTOsMap(Map<BlobStorageId,BlobStorageIOCost> srcs) {
		val res = new LinkedHashMap<String,BlobStorageIOCostDTO>();
		for(val e: srcs.entrySet()) {
			val id = e.getKey().id;
			val value = e.getValue().toDTO();
			res.put(id, value);
		}
		return res;
	}
	
	@Override
	public String toString() {
		return "{IOCost " //
				+ ((ioReadLen != 0)? "ioRead=" + ioReadLen : "") //
				+ ((ioWriteLen != 0)? " ioWrite=" + ioWriteLen : "") //
				+ ((callCount > 1)? " call=" + callCount : "") //
				+ ((metadataReadCount != 0)? " metadataRead=" + metadataReadCount : "") // 
				+ ((metadataWriteCount != 0)? " metadataWrite=" + metadataWriteCount : "") //
				+ "}";
	}

}
