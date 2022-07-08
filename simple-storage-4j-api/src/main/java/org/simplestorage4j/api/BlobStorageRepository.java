package org.simplestorage4j.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.val;

/**
 * repository equivalent to "Map<BlobStorageId, BlobStorage>"
 */
public class BlobStorageRepository {

	private Map<BlobStorageId,BlobStorage> blobStorages = new LinkedHashMap<>();

	// ------------------------------------------------------------------------
	
	public BlobStorageRepository() {
	}

	public BlobStorageRepository(Collection<BlobStorage> blobStorages) {
		addAll(blobStorages);
	}
	
	// ------------------------------------------------------------------------

	public List<BlobStorage> findAll() {
		return new ArrayList<>(this.blobStorages.values());
	}
	
	public BlobStorage findById(BlobStorageId id) {
		return this.blobStorages.get(id);
	}

	public BlobStorage get(BlobStorageId id) {
		val res = this.blobStorages.get(id);
		if (res == null) {
			throw new IllegalArgumentException("BlobStorage not found by id: " + id);
		}
		return res;
	}

	public void addAll(Collection<BlobStorage> blobStorages) {
		for(val blobStorage: blobStorages) {
			this.blobStorages.put(blobStorage.id, blobStorage);
		}
	}

	public void add(BlobStorage blobStorage) {
		this.blobStorages.put(blobStorage.id, blobStorage);
	}

	public void remove(BlobStorage blobStorage) {
		this.blobStorages.remove(blobStorage.id);
	}

}
