package org.simplestorage4j.sync.ops;

import org.simplestorage4j.api.BlobStorageId;

import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BlobStorageOperationExecutionResult {

	public final long startTime;
	public final long endTime;

	public final String errorMessage;
	public final Exception exception;
	
	public final ImmutableMap<BlobStorageId,BlobStorageOperationCounter> counterPerStorage;
}
