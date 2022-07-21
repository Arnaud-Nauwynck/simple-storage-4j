package org.simplestorage4j.opsserver.rest;

import java.util.List;

import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationErrorDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationWarningDTO;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationError;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationWarning;
import org.simplestorage4j.opscommon.dto.queue.AddJobOpsQueueRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.AddJobOpsQueueResponseDTO;
import org.simplestorage4j.opscommon.dto.queue.AddOpsToJobQueueRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.JobQueueInfoDTO;
import org.simplestorage4j.opscommon.dto.queue.JobQueueStatsDTO;
import org.simplestorage4j.opsserver.service.StorageJobOpsQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.val;

@RestController
@RequestMapping(path="/api/storage-ops/job-queue")
@OpenAPIDefinition(
		tags = { @Tag(name="jobqueue") }
		)
public class StorageJobOpsQueueRestController {

	@Autowired
	private StorageJobOpsQueueService jobOpsQueueService;
	
	// ------------------------------------------------------------------------
	
	@PutMapping()
	@Operation(description = "create Job Queue")
	public AddJobOpsQueueResponseDTO createJobQueue(@RequestBody AddJobOpsQueueRequestDTO req) {
		val res = jobOpsQueueService.createJobQueue(req);
		return res;
	}
	
	@DeleteMapping("/{jobId}")
	@Operation(description = "delete Job Queue")
	public void deleteJobQueue(@PathVariable("jobId") long jobId) {
		jobOpsQueueService.deleteJobQueue(jobId);
	}

	@PutMapping("/{jobId}/suspend")
	@Operation(description = "suspend Job Queue")
	public void suspendJobQueue(@PathVariable("jobId") long jobId) {
		jobOpsQueueService.suspendJobQueue(jobId);
	}

	@PutMapping("/{jobId}/resume")
	public void resumeJobQueue(@PathVariable("jobId") long jobId) {
		jobOpsQueueService.resumeJobQueue(jobId);
	}
	
	@PutMapping("/add-ops-to-job-queue")
	public void addOpsToJobQueue(@RequestBody AddOpsToJobQueueRequestDTO req) {
		jobOpsQueueService.addOpsToJobQueue(req);
	}

	@GetMapping("/{jobId}/info")
	public JobQueueInfoDTO getJobQueueInfo(@PathVariable("jobId") long jobId) {
		val res = jobOpsQueueService.getJobQueueInfo(jobId);
		return res;
	}

	@GetMapping("/infos")
	public List<JobQueueInfoDTO> getJobQueueInfos() {
		val res = jobOpsQueueService.getJobQueueInfos();
		return res;
	}
	
	@GetMapping("/{jobId}/stats")
	public JobQueueStatsDTO getJobQueueStats(@PathVariable("jobId") long jobId) {
		val res = jobOpsQueueService.getJobQueueStats(jobId);
		return res;
	}

	@GetMapping("/all-queues-stats")
	public List<JobQueueStatsDTO> getAllJobQueuesStats() {
		val res = jobOpsQueueService.getAllJobQueuesStats();
		return res;
	}

	@GetMapping("/active-queues-stats")
	public List<JobQueueStatsDTO> getActiveJobQueuesStats() {
		val res = jobOpsQueueService.getActiveJobQueuesStats();
		return res;
	}

	@GetMapping("/{jobId}/remain-ops")
	public List<BlobStorageOperationDTO> listJobQueueRemainOps(@PathVariable("jobId") long jobId,
			@RequestParam(name = "max", required = false, defaultValue = "200") int max
			) {
		val allRemainOps = jobOpsQueueService.listJobQueueRemainOps(jobId);
		val remainOps = (allRemainOps.size() > max)? allRemainOps.subList(0, max-1) : allRemainOps;
		val res = BlobStorageOperation.toDTOs(remainOps);
		return res;
	}
	
	@GetMapping("/{jobId}/errors")
	public List<BlobStorageOperationErrorDTO> listJobQueueErrors(
			@PathVariable("jobId") long jobId
			) {
		val tmpres = jobOpsQueueService.listJobQueueErrors(jobId);
		return BlobStorageOperationError.toDTOs(tmpres);
	}

	@GetMapping("/{jobId}/warnings")
	public List<BlobStorageOperationWarningDTO> listJobQueueWarnings(
			@PathVariable("jobId") long jobId
			) {
		val tmpres = jobOpsQueueService.listJobQueueWarnings(jobId);
		return BlobStorageOperationWarning.toDTOs(tmpres);
	}

	@GetMapping("/errors")
	public List<BlobStorageOperationErrorDTO> listJobQueuesErrors() {
		val tmpres = jobOpsQueueService.listJobQueuesErrors();
		return BlobStorageOperationError.toDTOs(tmpres);
	}

	@GetMapping("/warnings")
	public List<BlobStorageOperationWarningDTO> listJobQueuesWarnings() {
		val tmpres = jobOpsQueueService.listJobQueuesWarnings();
		return BlobStorageOperationWarning.toDTOs(tmpres);
	}

}
