package org.simplestorage4j.api.ops.encoder;

import java.io.BufferedReader;
import java.io.IOException;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;

import com.google.common.collect.ImmutableList;

import lombok.val;

public class BlobStorageOpErrorFormatter {

	public final long jobId;
	
	public BlobStorageOpErrorFormatter(long jobId) {
		this.jobId = jobId;
	}

	public String format(BlobStorageOperationResult result, BlobStorageOperation op) {
		val sb = new StringBuilder(100);
		sb.append(Long.toString(result.taskId));
		sb.append(':');
		sb.append(Long.toString(result.startTime));
		sb.append(':');
		sb.append(Long.toString(result.elapsedMillis));
		sb.append(':');
		if (result.warnings != null) {
			sb.append(result.warnings.toString().replaceAll(":", "_"));
		}
		// TOADD nbRetry
		sb.append(':');
		if (result.errorMessage != null) {
			sb.append(result.errorMessage.replaceAll(":", "_"));
		}
		sb.append(':');
		if (result.exception != null) {
			String exMessage = result.exception.getMessage();
			sb.append(exMessage.replaceAll(":", "_"));
		}
		sb.append(':');
		
		result.ioTimePerStorage.toTextLine(sb, ';', ',');
		sb.append('\n');
		val line = sb.toString();
		return line;
	}

	public BlobStorageOperationResult parse(BufferedReader reader) throws IOException {
		val line = reader.readLine();
		if (line == null) {
			return null;
		}
		val fields = line.split(":");
		val taskId = Long.parseLong(fields[0]);
		val startTime = Long.parseLong(fields[1]);
		val elapsedMillis = Long.parseLong(fields[2]);
		val warnsText = fields[3];
		ImmutableList<String> warnings = ImmutableList.of();
		if (warnsText != null && !warnsText.isEmpty()) {
			warnings = ImmutableList.copyOf(warnsText.split(","));
		}
		val errorText = fields[4];
		val errorMessage = errorText; // toadd: unescape chars?
		val exceptionText = fields[5];
		val exception = (exceptionText != null)? new RuntimeException(exceptionText) : null;
		val ioTimePerStoragesText = fields[6];
		val ioTimePerStorage = PerBlobStoragesIOTimeResult.parseFromTextLine(ioTimePerStoragesText, ';', ',');
		return new BlobStorageOperationResult(jobId, taskId, startTime, elapsedMillis, //
				warnings, errorMessage, exception, //
				ioTimePerStorage);
	}

}
