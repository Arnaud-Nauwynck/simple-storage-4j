package org.simplestorage4j.api.iocost.counter;

import java.util.HashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;

import lombok.val;

public class PerBlobStoragesIOTimeCounter {

	private final Object lock = new Object();
	
	// @GuardedBy("lock")
	private final Map<BlobStorageId,BlobStorageIOTimeCounter> perStorages = new HashMap<>();

	// ------------------------------------------------------------------------
	
	public BlobStorageIOTimeCounter getOrCreateCounterFor(BlobStorageId id) {
		synchronized(lock) {
			return perStorages.computeIfAbsent(id, x -> new BlobStorageIOTimeCounter());
		}
	}
	
	public void incr(PerBlobStoragesIOTimeResult add) {
		for(val e : add.countPerStorage.entrySet()) {
			val id = e.getKey();
			val storageIncr = e.getValue();
			val counter = getOrCreateCounterFor(id);
			counter.incr(storageIncr);
		}
	}

}
