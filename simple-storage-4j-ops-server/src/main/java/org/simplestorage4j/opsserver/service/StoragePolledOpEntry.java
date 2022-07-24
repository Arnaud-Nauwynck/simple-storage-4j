package org.simplestorage4j.opsserver.service;

import org.simplestorage4j.api.ops.BlobStorageOperation;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StoragePolledOpEntry {
	public final StorageOpsExecutorSessionEntry owner;
	public final BlobStorageOperation op;
	public final long polledTime;
	
}