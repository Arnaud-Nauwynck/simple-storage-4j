package org.simplestorage4j.opsserver.service;

import org.simplestorage4j.api.ops.BlobStorageOperation;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PolledJobTaskEntry {
	public final ExecutorSessionEntry owner;
	public final long jobId;
	public final BlobStorageOperation op;
	public final long polledStartTime;
	
}