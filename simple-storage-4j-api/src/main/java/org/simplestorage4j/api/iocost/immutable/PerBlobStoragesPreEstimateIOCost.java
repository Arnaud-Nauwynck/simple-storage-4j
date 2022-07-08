package org.simplestorage4j.api.iocost.immutable;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.PerBlobStoragesPreEstimateIOCostDTO;

import com.google.common.collect.ImmutableMap;

import lombok.Getter;
import lombok.val;

@Getter
public class PerBlobStoragesPreEstimateIOCost {

	public final ImmutableMap<BlobStorageId,BlobStoragePreEstimateIOCost> perStorage;

	// ------------------------------------------------------------------------
	
	public PerBlobStoragesPreEstimateIOCost(@Nonnull ImmutableMap<BlobStorageId, BlobStoragePreEstimateIOCost> perStorage) {
		this.perStorage = Objects.requireNonNull(perStorage);
	}
	
	public static PerBlobStoragesPreEstimateIOCost of(BlobStorage storage, BlobStoragePreEstimateIOCost ioCost) {
		return new PerBlobStoragesPreEstimateIOCost(ImmutableMap.of(storage.id, ioCost));
	}

	public static PerBlobStoragesPreEstimateIOCost of(
			BlobStorage storage1, BlobStoragePreEstimateIOCost ioCost1,
			BlobStorage storage2, BlobStoragePreEstimateIOCost ioCost2
			) {
		return new PerBlobStoragesPreEstimateIOCost(ImmutableMap.of(storage1.id, ioCost1, storage2.id, ioCost2));
	}

	// ------------------------------------------------------------------------
	
	public static PerBlobStoragesPreEstimateIOCost fromDTO(PerBlobStoragesPreEstimateIOCostDTO src) {
		val perStorage = BlobStoragePreEstimateIOCost.fromDTOsMap(src.perStorages);
		return new PerBlobStoragesPreEstimateIOCost(perStorage);
	}
	
	// ------------------------------------------------------------------------
	
	public PerBlobStoragesPreEstimateIOCostDTO toDTO() {
		val perStorageDTOs = BlobStoragePreEstimateIOCost.toDTOsMap(perStorage);
		return new PerBlobStoragesPreEstimateIOCostDTO(perStorageDTOs);
	}
	
	@Override
	public String toString() {
		return "{OpCost perStorage:" + perStorage + "}";
	}
	
}
