package org.simplestorage4j.executor.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.simplestorage4j.api.util.BlobStorageNamedThreadFactory;

import lombok.Getter;
import lombok.val;

@Getter
public class StorageJobOpsExecutorsService {

	private ExecutorService opsExecutorService;
	
	private ExecutorService opsSubTasksExecutorService;
	
	private ExecutorService opsLargeFileRangeExecutorService;

	// ------------------------------------------------------------------------
	
	public StorageJobOpsExecutorsService(
			ExecutorService opsExecutorService, 
			ExecutorService opsSubTasksExecutorService,
			ExecutorService opsLargeFileRangeExecutorService) {
		this.opsExecutorService = opsExecutorService;
		this.opsSubTasksExecutorService = opsSubTasksExecutorService;
		this.opsLargeFileRangeExecutorService = opsLargeFileRangeExecutorService;
	}

	public static StorageJobOpsExecutorsService createDefault(
		int opsThreadCount,
		int subTasksThreadCount,
		int largeFileRangeThreadCount) {
		val opsExecutorService = Executors.newFixedThreadPool(opsThreadCount, 
				new BlobStorageNamedThreadFactory("ops-executor-", "", true)); 
		val opsSubTasksExecutorService = Executors.newFixedThreadPool(opsThreadCount, 
				new BlobStorageNamedThreadFactory("ops-sub-tasks-", "", true)); 
		val opsLargeFileRangeExecutorService = Executors.newFixedThreadPool(opsThreadCount, 
				new BlobStorageNamedThreadFactory("ops-large-file-", "", true)); 
		return new StorageJobOpsExecutorsService(opsExecutorService, opsSubTasksExecutorService, opsLargeFileRangeExecutorService);
	}

	// ------------------------------------------------------------------------
	
	public <T> Future<T> submitOpTask(Callable<T> task) {
		return opsExecutorService.submit(task);
	}

	public <T> Future<T> submitOpSubTask(Callable<T> task) {
		return opsSubTasksExecutorService.submit(task);
	}

	public <T> Future<T> submitOpLargeFileRangeTask(Callable<T> task) {
		return opsLargeFileRangeExecutorService.submit(task);
	}

}
