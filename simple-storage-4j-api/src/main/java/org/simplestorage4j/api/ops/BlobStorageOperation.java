package org.simplestorage4j.api.ops;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;

import lombok.Getter;

@Getter
public abstract class BlobStorageOperation {

	public final long jobId; 
    public final long taskId;
    
//    TODO 
//    private List<BlobStorageOperation> dependencies;
    
    // --------------------------------------------------------------------------------------------
    
    public BlobStorageOperation(long jobId, long taskId) {
        this.jobId = jobId;
    	this.taskId = taskId;
    }

    // --------------------------------------------------------------------------------------------
    
    public abstract String taskTypeName();

    public abstract PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost();

    public abstract BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx);

    public abstract BlobStorageOperationDTO toDTO();

    public static List<BlobStorageOperationDTO> toDTOs(Collection<BlobStorageOperation> ops) {
    	return ops.stream().map(x -> x.toDTO()).collect(Collectors.toList());
    }

}
