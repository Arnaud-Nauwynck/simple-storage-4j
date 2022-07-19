package org.simplestorage4j.api.iocost.dto;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO class for BlobStorage operation execution result
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class BlobStorageOperationResultDTO implements Serializable {

	/** for java.io.Serializable */
	private static final long serialVersionUID = 1L;

	public long jobId;
	public long taskId;

	public long startTime;
	public long elapsedMillis;

	/** warning typically for truncated files => only a fragment is available to copy.. even after retries */
	public List<String> warnings;
	
	public String errorMessage;
	public String exception;
	
	public PerBlobStoragesIOTimeResultDTO ioTimePerStorage;

	@Override
	public String toString() {
		return "{" //
				+ "taskResult " + jobId + ":" + taskId //
				// + ", startTime=" + startTime //
				+ ", elapsedMillis=" + elapsedMillis //
				+ ((warnings != null)? ", warnings=" + warnings : "") //
				+ ((errorMessage != null)? ", errorMessage=" + errorMessage : "") //
				+ ((exception != null)? ", exception=" + exception : "") //
				+ ", ioTimePerStorage=" + ioTimePerStorage //
				+ "}";
	}

}
