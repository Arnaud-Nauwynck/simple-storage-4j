package org.simplestorage4j.api.ops.encoder;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;

import com.google.common.collect.ImmutableList;

import lombok.val;

public class BlobStorageOpWarningFormatter {

	public final long jobId;
	
	public BlobStorageOpWarningFormatter(long jobId) {
		this.jobId = jobId;
	}

	public String format(BlobStorageOperationResult result) {
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
		sb.append(':');
		// .. errorMessage, exception.. not set (should be null here)
		result.ioTimePerStorage.toTextLine(sb, ';', ',');
		sb.append('\n');
		val line = sb.toString();
		return line;
	}

	public BlobStorageOperationResult parse(String line) {
		val fields = line.split(":");
		val taskId = Long.parseLong(fields[0]);
		val startTime = Long.parseLong(fields[1]);
		val elapsedMillis = Long.parseLong(fields[2]);
		val warnsText = fields[3];
		ImmutableList<String> warnings = ImmutableList.of();
		if (warnsText != null && !warnsText.isEmpty()) {
			warnings = ImmutableList.copyOf(warnsText.split(","));
		}
		// .. errorMessage, exception.. not set (should be null here)
		val ioTimePerStoragesText = fields[4];
		val ioTimePerStorage = PerBlobStoragesIOTimeResult.parseFromTextLine(ioTimePerStoragesText, ';', ',');
		return new BlobStorageOperationResult(jobId, taskId, startTime, elapsedMillis, //
				warnings, null, null, // warnings, errorMessage, exception
				ioTimePerStorage);
	}

}
