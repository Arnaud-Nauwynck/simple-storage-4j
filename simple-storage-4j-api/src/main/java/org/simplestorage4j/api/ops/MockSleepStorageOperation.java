package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.immutable.BlobStorageIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.MockSleepStorageOperationDTO;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
@Getter
public class MockSleepStorageOperation extends BlobStorageOperation {

	public final int mockDurationMillis;
	public final BlobStorageId destStorageId;
	public final BlobStorageId srcStorageId;
	public final long mockDestFileLen;
	public final long mockSrcFileLen;

	// ------------------------------------------------------------------------
	
    public MockSleepStorageOperation(long jobId, long taskId, //
    		int mockDurationMillis,
    		BlobStorageId destStorageId,
    		BlobStorageId srcStorageId,
    		long mockDestFileLen,
			long mockSrcFileLen
    		) {
        super(jobId, taskId);
        this.mockDurationMillis = mockDurationMillis;
        this.destStorageId = destStorageId;
        this.srcStorageId = srcStorageId;
        this.mockDestFileLen = mockDestFileLen;
        this.mockSrcFileLen = mockSrcFileLen;
    }

    // ------------------------------------------------------------------------

    @Override
    public String taskTypeName() {
        return "mock-sleep";
    }

    @Override
	public PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost() {
		val costs = ImmutableMap.<BlobStorageId, BlobStoragePreEstimateIOCost>builder();
		if (srcStorageId != null) {
			costs.put(srcStorageId, BlobStoragePreEstimateIOCost.ofIoRead1(mockSrcFileLen));
		}
		if (destStorageId != null && (srcStorageId == null || ! srcStorageId.equals(destStorageId))) {
			costs.put(destStorageId, BlobStoragePreEstimateIOCost.ofIoWrite1(mockDestFileLen));
		}
		return new PerBlobStoragesPreEstimateIOCost(costs.build());
	}

	@Override
	public BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx) {
		val startTime = System.currentTimeMillis();
		
		try {
			Thread.sleep(mockDurationMillis);
		} catch(InterruptedException ex) {
			// ignore
		}
		
		val millis = System.currentTimeMillis() - startTime;
		val ioTimes = ImmutableMap.<BlobStorageId, BlobStorageIOTimeResult>builder();
		if (srcStorageId != null) {
			ioTimes.put(srcStorageId, BlobStorageIOTimeResult.ofIoRead1(millis/2, mockSrcFileLen));
		}
		if (destStorageId != null && (srcStorageId == null || ! srcStorageId.equals(destStorageId))) {
			ioTimes.put(destStorageId, BlobStorageIOTimeResult.ofIoWrite1(millis/2, mockDestFileLen));
		}
		val res = new BlobStorageOperationResult(jobId, taskId, startTime, millis, //
				ImmutableList.of(), null, null, //
				new PerBlobStoragesIOTimeResult(ioTimes.build()));
		ctx.logIncr_mockSleep(this, res, logPrefix -> log.info(logPrefix + "(" + mockDurationMillis + ")"));
		return res;
	}

	@Override
    public BlobStorageOperationDTO toDTO() {
    	return new MockSleepStorageOperationDTO(jobId, taskId, //
    			mockDurationMillis,
    			((srcStorageId != null)? srcStorageId.id : null),
    			((destStorageId != null)? destStorageId.id : null),
    			mockSrcFileLen, mockDestFileLen);
    }

	@Override
    public String toString() {
        return "{mock-sleep " + taskId //
				+ mockDurationMillis + " ms" //
				+ ((destStorageId != null)? " destStorageId:" + destStorageId : "")
				+ ((srcStorageId != null)? " srcStorageId:" + srcStorageId : "")
				+ ((mockDestFileLen != 0 && mockDestFileLen != mockSrcFileLen)? " mockDestFileLen:" + mockSrcFileLen : "")
				+ ((mockSrcFileLen != 0)? " mockSrcFileLen:" + mockSrcFileLen : "")
        		+ "}";
    }

}
