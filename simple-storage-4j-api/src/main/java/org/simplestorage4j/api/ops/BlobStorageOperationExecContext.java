package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
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
			PerBlobStoragesIOTimeResult ioTimeResut,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_mkdir.logIncr(ioTimeResut, msgPrefixLoggingCallback);
	}
	
	public void logIncr_copyFile(
			CopyFileStorageOperation op,
			PerBlobStoragesIOTimeResult ioTimeResut,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_copyFile.logIncr(ioTimeResut, msgPrefixLoggingCallback);
	}

	public void logIncr_copyFileContent(
			CopyFileContentStorageOperation op,
			PerBlobStoragesIOTimeResult ioTimeResut,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_copyFileContent.logIncr(ioTimeResut, msgPrefixLoggingCallback);
	}

	public void logIncr_zipCopyFile(
			ZipCopyFileStorageOperation op,
			PerBlobStoragesIOTimeResult ioTimeResut,
			MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		loggingCounter_zipCopyFile.logIncr(ioTimeResut, msgPrefixLoggingCallback);
	}

}
