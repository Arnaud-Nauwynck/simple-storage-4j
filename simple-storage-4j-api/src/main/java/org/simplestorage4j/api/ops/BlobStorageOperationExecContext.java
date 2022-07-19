package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.util.LoggingCounter.LoggingCounterParams;
import org.simplestorage4j.api.util.LoggingCounter.MsgPrefixLoggingCallback;

import lombok.Getter;

public class BlobStorageOperationExecContext {

	@Getter
	private final BlobStorageOperationsIOLoggingCounter loggingCounter_mkdir;

	@Getter
	private final BlobStorageOperationsIOLoggingCounter loggingCounter_copyFile;

	@Getter
	private final BlobStorageOperationsIOLoggingCounter loggingCounter_copyFileContent;

	@Getter
	private final BlobStorageOperationsIOLoggingCounter loggingCounter_zipCopyFile;
	
	
	// ------------------------------------------------------------------------

	public BlobStorageOperationExecContext() {
		this("","", new LoggingCounterParams());
	}

	public BlobStorageOperationExecContext(String msgPrefix, String msgSuffix,
			LoggingCounterParams logParams) {
		this.loggingCounter_mkdir = new BlobStorageOperationsIOLoggingCounter(msgPrefix + "mkdir" + msgSuffix, logParams);
		this.loggingCounter_copyFile = new BlobStorageOperationsIOLoggingCounter(msgPrefix + "copyFile" + msgSuffix, logParams);
		this.loggingCounter_copyFileContent = new BlobStorageOperationsIOLoggingCounter(msgPrefix + "copyFileContent" + msgSuffix, logParams);
		this.loggingCounter_zipCopyFile = new BlobStorageOperationsIOLoggingCounter(msgPrefix + "zipCopyFile" + msgSuffix, logParams);
	}
	
	// ------------------------------------------------------------------------
	
	public void logIncr_mkdir(
			MkdirStorageOperation op,
			BlobStorageOperationResult opResult,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_mkdir.logIncr(opResult, msgPrefixLoggingCallback);
	}
	
	public void logIncr_copyFile(
			CopyFileStorageOperation op,
			BlobStorageOperationResult opResult,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_copyFile.logIncr(opResult, msgPrefixLoggingCallback);
	}

	public void logIncr_copyFileContent(
			CopyFileContentStorageOperation op,
			BlobStorageOperationResult opResult,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_copyFileContent.logIncr(opResult, msgPrefixLoggingCallback);
	}

	public void logIncr_zipCopyFile(
			ZipCopyFileStorageOperation op,
			BlobStorageOperationResult opResult,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_zipCopyFile.logIncr(opResult, msgPrefixLoggingCallback);
	}

}
