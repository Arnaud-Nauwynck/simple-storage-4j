package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.MockSleepStorageOperationDTO;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class MockSleepStorageOperation extends BlobStorageOperation {

	private int mockDurationMillis;
	
	// ------------------------------------------------------------------------
	
    public MockSleepStorageOperation(long jobId, long taskId, //
    		int mockDurationMillis) {
        super(jobId, taskId);
        this.mockDurationMillis = mockDurationMillis;
    }

    // ------------------------------------------------------------------------

    @Override
    public String taskTypeName() {
        return "mock-sleep";
    }

    @Override
	public PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost() {
		return new PerBlobStoragesPreEstimateIOCost(ImmutableMap.of());
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
		val res = new BlobStorageOperationResult(jobId, taskId, startTime, millis, //
				ImmutableList.of(), null, null, //
				new PerBlobStoragesIOTimeResult(ImmutableMap.of()));
		ctx.logIncr_mockSleep(this, res, logPrefix -> log.info(logPrefix + "(" + mockDurationMillis + ")"));
		return res;
	}

	@Override
    public BlobStorageOperationDTO toDTO() {
    	return new MockSleepStorageOperationDTO(jobId, taskId, mockDurationMillis);
    }

	@Override
    public String toString() {
        return "{mock-sleep " + taskId //
        		+ " " + mockDurationMillis //
        		+ "}";
    }

}
