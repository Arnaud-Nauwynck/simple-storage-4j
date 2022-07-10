package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.iocost.counter.PerBlobStoragesIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
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
			PerBlobStoragesIOTimeResult ioTimeResut, MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter.incr(ioTimeResut.elapsedMillis, msgPrefixLoggingCallback);
		ioCounter.incr(ioTimeResut);
	}
	
}