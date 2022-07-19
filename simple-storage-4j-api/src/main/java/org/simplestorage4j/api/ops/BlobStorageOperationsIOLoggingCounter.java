package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.iocost.counter.PerBlobStoragesIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.util.LoggingCounter;
import org.simplestorage4j.api.util.LoggingCounter.LoggingCounterParams;
import org.simplestorage4j.api.util.LoggingCounter.MsgPrefixLoggingCallback;

import lombok.Getter;

public class BlobStorageOperationsIOLoggingCounter {
	
	@Getter
	private final LoggingCounter loggingCounter;

	@Getter
	private final PerBlobStoragesIOTimeCounter ioCounter = new PerBlobStoragesIOTimeCounter(); 
	
	public BlobStorageOperationsIOLoggingCounter(String logDisplayMsg, LoggingCounterParams logParams) {
		this.loggingCounter = new LoggingCounter(logDisplayMsg, logParams);
	}
	
	public void logIncr(
			BlobStorageOperationResult opResult, MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter.incr(opResult.elapsedMillis, msgPrefixLoggingCallback);
		ioCounter.incr(opResult.ioTimePerStorage);
	}
	
}