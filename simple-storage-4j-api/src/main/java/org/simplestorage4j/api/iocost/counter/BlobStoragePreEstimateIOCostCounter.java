package org.simplestorage4j.api.iocost.counter;

import java.util.LinkedHashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.BlobStoragePreEstimateIOCostDTO;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;

import com.google.common.collect.ImmutableMap;

import lombok.val;

public class BlobStoragePreEstimateIOCostCounter {
	
	private long ioReadLen;
	private long ioWriteLen;
	private int callCount;
	private int metadataReadCount;
	private int metadataWriteCount;
	
	// ------------------------------------------------------------------------
	
	public BlobStoragePreEstimateIOCostCounter() {
	}
	
	public BlobStoragePreEstimateIOCostCounter(long ioReadLen, long ioWriteLen, int callCount, int metadataReadCount,
			int metadataWriteCount) {
		this.ioReadLen = ioReadLen;
		this.ioWriteLen = ioWriteLen;
		this.callCount = callCount;
		this.metadataReadCount = metadataReadCount;
		this.metadataWriteCount = metadataWriteCount;
	}

	public static BlobStoragePreEstimateIOCostCounter fromDTO(BlobStoragePreEstimateIOCostDTO src) {
		return new BlobStoragePreEstimateIOCostCounter(
				src.ioReadLen, src.ioWriteLen, src.callCount, src.metadataReadCount, src.metadataWriteCount);
	}
	
	public static Map<BlobStorageId,BlobStoragePreEstimateIOCostCounter> fromDTOsMap(Map<String,BlobStoragePreEstimateIOCostDTO> srcs) {
		val res = new LinkedHashMap<BlobStorageId,BlobStoragePreEstimateIOCostCounter>();
		if (srcs != null) {
			for(val e : srcs.entrySet()) {
				val id = new BlobStorageId(e.getKey());
				val value = fromDTO(e.getValue());
				res.put(id, value);
			}
		}
		return res;
	}
	
	// ------------------------------------------------------------------------
	
	public synchronized void incr(BlobStoragePreEstimateIOCostDTO src) {
		this.ioReadLen += src.ioReadLen;
		this.ioWriteLen += src.ioWriteLen;
		this.callCount += src.callCount;
		this.metadataReadCount += src.metadataReadCount;
		this.metadataWriteCount += src.metadataWriteCount;
	}

	public synchronized void incr(BlobStoragePreEstimateIOCost src) {
		this.ioReadLen += src.ioReadLen;
		this.ioWriteLen += src.ioWriteLen;
		this.callCount += src.callCount;
		this.metadataReadCount += src.metadataReadCount;
		this.metadataWriteCount += src.metadataWriteCount;
	}

	public synchronized void decr(BlobStoragePreEstimateIOCostDTO src) {
		this.ioReadLen -= src.ioReadLen;
		this.ioWriteLen -= src.ioWriteLen;
		this.callCount -= src.callCount;
		this.metadataReadCount -= src.metadataReadCount;
		this.metadataWriteCount -= src.metadataWriteCount;
	}
	
	public synchronized void decr(BlobStoragePreEstimateIOCost src) {
		this.ioReadLen -= src.ioReadLen;
		this.ioWriteLen -= src.ioWriteLen;
		this.callCount -= src.callCount;
		this.metadataReadCount -= src.metadataReadCount;
		this.metadataWriteCount -= src.metadataWriteCount;
	}

	public BlobStoragePreEstimateIOCost toImmutable() {
		return new BlobStoragePreEstimateIOCost(ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount);
	}
	
	public BlobStoragePreEstimateIOCostDTO toDTO() {
		return new BlobStoragePreEstimateIOCostDTO(ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount);
	}

	public static ImmutableMap<String,BlobStoragePreEstimateIOCost> toImmutable(Map<BlobStorageId,BlobStoragePreEstimateIOCostCounter> srcs) {
		val res = ImmutableMap.<String,BlobStoragePreEstimateIOCost>builder();
		for(val e: srcs.entrySet()) {
			val id = e.getKey().id;
			val value = e.getValue().toImmutable();
			res.put(id, value);
		}
		return res.build();
	}

	public static Map<String,BlobStoragePreEstimateIOCostDTO> toDTOsMap(Map<BlobStorageId,BlobStoragePreEstimateIOCostCounter> srcs) {
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
