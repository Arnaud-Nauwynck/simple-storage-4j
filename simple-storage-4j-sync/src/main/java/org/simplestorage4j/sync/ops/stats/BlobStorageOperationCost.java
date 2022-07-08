package org.simplestorage4j.sync.ops.stats;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.sync.ops.dto.BlobStorageOperationCostDTO;

import com.google.common.collect.ImmutableMap;

import lombok.Getter;
import lombok.val;

@Getter
public class BlobStorageOperationCost {

	public final ImmutableMap<BlobStorageId,BlobStorageIOCost> perStorage;

	// ------------------------------------------------------------------------
	
	public BlobStorageOperationCost(@Nonnull ImmutableMap<BlobStorageId, BlobStorageIOCost> perStorage) {
		this.perStorage = Objects.requireNonNull(perStorage);
	}
	
	public static BlobStorageOperationCost of(BlobStorage storage, BlobStorageIOCost ioCost) {
		return new BlobStorageOperationCost(ImmutableMap.of(storage.id, ioCost));
	}

	public static BlobStorageOperationCost of(
			BlobStorage storage1, BlobStorageIOCost ioCost1,
			BlobStorage storage2, BlobStorageIOCost ioCost2
			) {
		return new BlobStorageOperationCost(ImmutableMap.of(storage1.id, ioCost1, storage2.id, ioCost2));
	}

	// ------------------------------------------------------------------------
	
	public static BlobStorageOperationCost fromDTO(BlobStorageOperationCostDTO src) {
		val perStorage = BlobStorageIOCost.fromDTOsMap(src.perStorage);
		return new BlobStorageOperationCost(perStorage);
	}
	
	// ------------------------------------------------------------------------
	
	public BlobStorageOperationCostDTO toDTO() {
		val perStorageDTOs = BlobStorageIOCost.toDTOsMap(perStorage);
		return new BlobStorageOperationCostDTO(perStorageDTOs);
	}
	
	@Override
	public String toString() {
		return "{OpCost perStorage:" + perStorage + "}";
	}
	
}
