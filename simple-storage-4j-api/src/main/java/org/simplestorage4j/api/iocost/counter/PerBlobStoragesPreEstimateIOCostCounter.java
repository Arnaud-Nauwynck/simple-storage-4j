package org.simplestorage4j.api.iocost.counter;

import java.util.HashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.BlobStoragePreEstimateIOCostDTO;
import org.simplestorage4j.api.iocost.dto.PerBlobStoragesPreEstimateIOCostDTO;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;

import lombok.val;

public class PerBlobStoragesPreEstimateIOCostCounter {

	private final Map<BlobStorageId,BlobStoragePreEstimateIOCostCounter> perStorage = new HashMap<>();

	// ------------------------------------------------------------------------
	
	public PerBlobStoragesPreEstimateIOCostCounter() {
	}
	
	public PerBlobStoragesPreEstimateIOCostCounter(Map<BlobStorageId,BlobStoragePreEstimateIOCostCounter> perStorage) {
		this.perStorage.putAll(perStorage);
	}

	// ------------------------------------------------------------------------
	
	protected BlobStoragePreEstimateIOCostCounter getOrCreateFor(BlobStorageId storageId) {
		synchronized(this) {
			return this.perStorage.computeIfAbsent(storageId, x -> new BlobStoragePreEstimateIOCostCounter());
		}
	}
	
	public void incr(PerBlobStoragesPreEstimateIOCost src) {
		for(val e : src.perStorage.entrySet()) {
			incr(e.getKey(), e.getValue());
		}
	}

	public void decr(PerBlobStoragesPreEstimateIOCost src) {
		for(val e : src.perStorage.entrySet()) {
			decr(e.getKey(), e.getValue());
		}
	}

	public void incr(PerBlobStoragesPreEstimateIOCostCounter src) {
		for(val e : src.perStorage.entrySet()) {
			incr(e.getKey(), e.getValue());
		}
	}

	public void decr(PerBlobStoragesPreEstimateIOCostCounter src) {
		for(val e : src.perStorage.entrySet()) {
			decr(e.getKey(), e.getValue());
		}
	}

	public void incr(PerBlobStoragesPreEstimateIOCostDTO src) {
		for(val e : src.perStorage.entrySet()) {
			incr(BlobStorageId.of(e.getKey()), e.getValue());
		}
	}

	public void decr(PerBlobStoragesPreEstimateIOCostDTO src) {
		for(val e : src.perStorage.entrySet()) {
			decr(BlobStorageId.of(e.getKey()), e.getValue());
		}
	}

	public void incr(BlobStorageId storageId, BlobStoragePreEstimateIOCost src) {
		val storageCounter = getOrCreateFor(storageId);
		storageCounter.incr(src);
	}

	public void decr(BlobStorageId storageId, BlobStoragePreEstimateIOCost src) {
		val storageCounter = getOrCreateFor(storageId);
		storageCounter.decr(src);
	}
	
	public void incr(BlobStorageId storageId, BlobStoragePreEstimateIOCostCounter src) {
		val storageCounter = getOrCreateFor(storageId);
		storageCounter.incr(src);
	}

	public void decr(BlobStorageId storageId, BlobStoragePreEstimateIOCostCounter src) {
		val storageCounter = getOrCreateFor(storageId);
		storageCounter.decr(src);
	}

	public void incr(BlobStorageId storageId, BlobStoragePreEstimateIOCostDTO src) {
		val storageCounter = getOrCreateFor(storageId);
		storageCounter.incr(src);
	}

	public void decr(BlobStorageId storageId, BlobStoragePreEstimateIOCostDTO src) {
		val storageCounter = getOrCreateFor(storageId);
		storageCounter.decr(src);
	}


	// ------------------------------------------------------------------------
	
	public static PerBlobStoragesPreEstimateIOCostCounter fromDTO(PerBlobStoragesPreEstimateIOCostDTO src) {
		val perStorage = BlobStoragePreEstimateIOCostCounter.fromDTOsMap(src.perStorage);
		return new PerBlobStoragesPreEstimateIOCostCounter(perStorage);
	}
	
	public PerBlobStoragesPreEstimateIOCostDTO toDTO() {
		val perStorageDTOs = BlobStoragePreEstimateIOCostCounter.toDTOsMap(perStorage);
		return new PerBlobStoragesPreEstimateIOCostDTO(perStorageDTOs);
	}
	
	@Override
	public String toString() {
		return "{PreEstimateIOCost perStorage:" + perStorage + "}";
	}

}
