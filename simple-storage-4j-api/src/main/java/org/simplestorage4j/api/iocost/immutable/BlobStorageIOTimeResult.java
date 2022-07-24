package org.simplestorage4j.api.iocost.immutable;

import java.util.LinkedHashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.BlobStorageIOTimeResultDTO;

import com.google.common.collect.ImmutableMap;

import lombok.val;

public class BlobStorageIOTimeResult {

	public final long elapsedTimeMillis;
	// may use ThreadCpuTime, threadUserTime, threadKernelTime ...
	
	public final long ioReadLen;
	public final long ioWriteLen;
	public final int callCount;
	public final int metadataReadCount;
	public final int metadataWriteCount;

	// ------------------------------------------------------------------------
	
	public BlobStorageIOTimeResult(long elapsedTimeMillis, 
			long ioReadLen, long ioWriteLen, int callCount,
			int metadataReadCount, int metadataWriteCount) {
		this.elapsedTimeMillis = elapsedTimeMillis;
		this.ioReadLen = ioReadLen;
		this.ioWriteLen = ioWriteLen;
		this.callCount = callCount;
		this.metadataReadCount = metadataReadCount;
		this.metadataWriteCount = metadataWriteCount;
	}

	public static BlobStorageIOTimeResult of(long elapsedTimeMillis, 
			long ioReadLen, long ioWriteLen, int callCount,
			int metadataReadCount, int metadataWriteCount) {
		return new BlobStorageIOTimeResult(elapsedTimeMillis, ioReadLen, ioWriteLen,
				callCount, metadataReadCount, metadataWriteCount);
	}

	public static BlobStorageIOTimeResult ofIoReadN(long elapsedMillis, long ioReadLen, int callCount) {
		return of(elapsedMillis, ioReadLen, 0L, callCount, 0, 0);
	}

	public static BlobStorageIOTimeResult ofIoRead1(long elapsedMillis, long ioReadLen) {
		return of(elapsedMillis, ioReadLen, 0L, 1, 0, 0);
	}

	public static BlobStorageIOTimeResult ofIoWriteN(long elapsedMillis, long ioWriteLen, int callCount) {
		return of(elapsedMillis, 0L, ioWriteLen, callCount, 0, 0);
	}

	public static BlobStorageIOTimeResult ofIoWrite1(long elapsedMillis, long ioWriteLen) {
		return of(elapsedMillis, 0L, ioWriteLen, 1, 0, 0);
	}
	
	public static BlobStorageIOTimeResult ofSum(BlobStorageIOTimeResult left, BlobStorageIOTimeResult right) {
		return new BlobStorageIOTimeResult(
				left.elapsedTimeMillis + right.elapsedTimeMillis,
				left.ioReadLen + right.ioReadLen,
				left.ioWriteLen + right.ioWriteLen,
				left.callCount + right.callCount,
				left.metadataReadCount + right.metadataReadCount,
				left.metadataWriteCount + right.metadataWriteCount
				);
	}
	
	public BlobStorageIOTimeResultDTO toDTO() {
		return new BlobStorageIOTimeResultDTO(
				elapsedTimeMillis, ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount);
	}

	public static BlobStorageIOTimeResult fromDTO(BlobStorageIOTimeResultDTO src) {
		return new BlobStorageIOTimeResult(
				src.elapsedTimeMillis, src.ioReadLen, src.ioWriteLen, src.callCount, src.metadataReadCount, src.metadataWriteCount);
	}

	public static Map<String,BlobStorageIOTimeResultDTO> toDTOsMap(Map<BlobStorageId,BlobStorageIOTimeResult> src) {
		val res = new LinkedHashMap<String,BlobStorageIOTimeResultDTO>();
		for(val e: src.entrySet()) {
			val id = e.getKey().id;
			val dto = e.getValue().toDTO();
			res.put(id, dto);
		}
		return res;
	}

	public static ImmutableMap<BlobStorageId,BlobStorageIOTimeResult> fromDTOsMap(
			Map<String,BlobStorageIOTimeResultDTO> src) {
		val res = ImmutableMap.<BlobStorageId,BlobStorageIOTimeResult>builder();
		for(val e: src.entrySet()) {
			val id = new BlobStorageId(e.getKey());
			val value = fromDTO(e.getValue());
			res.put(id, value);
		}
		return res.build();
	}

	public void toTextLine(StringBuilder sb, char sep) {
		sb.append(Long.toString(elapsedTimeMillis));
		sb.append(sep);
		// may use ThreadCpuTime, threadUserTime, threadKernelTime ...
		sb.append(Long.toString(ioReadLen));
		sb.append(sep);
		sb.append(Long.toString(ioWriteLen));
		sb.append(sep);
		sb.append(Integer.toString(callCount));
		sb.append(sep);
		sb.append(Integer.toString(metadataReadCount));
		sb.append(sep);
		sb.append(Integer.toString(metadataWriteCount));
		sb.append(sep);		
	}

	public static BlobStorageIOTimeResult parseFromTextLine(String line, char sep) {
		val fields = line.split(Character.toString(sep));
		val elapsedTimeMillis = Long.parseLong(fields[0]);
		// may use ThreadCpuTime, threadUserTime, threadKernelTime ...
		val ioReadLen = Long.parseLong(fields[1]);
		val ioWriteLen = Long.parseLong(fields[2]);
		val callCount = Integer.parseInt(fields[3]);
		val metadataReadCount = Integer.parseInt(fields[4]);
		val metadataWriteCount = Integer.parseInt(fields[5]);
		return new BlobStorageIOTimeResult(elapsedTimeMillis, ioReadLen, ioWriteLen, 
				callCount, metadataReadCount, metadataWriteCount);
	}

	@Override
	public String toString() {
		return "{BlobStorageIOTimeResult " //
				+ ((elapsedTimeMillis != 0)? "elapsed=" + elapsedTimeMillis : "") //
				+ ((ioReadLen != 0)? ", ioReadLen=" + ioReadLen : "") //
				+ ((ioWriteLen != 0)? ", ioWriteLen=" + ioWriteLen : "") //
				+ ((callCount > 1)? ", callCount=" + callCount : "") //
				+ ((metadataReadCount != 0)? ", metadataReadCount=" + metadataReadCount : "") //
				+ ((metadataWriteCount != 0)? ", metadataWriteCount=" + metadataWriteCount : "") //
				+ "}";
	}

}
