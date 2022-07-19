package org.simplestorage4j.opsserver.rest;

import java.util.List;

import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
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

import lombok.val;

@RestController
@RequestMapping(path="/api/storage-ops/job-queue")
public class StorageJobOpsQueueRestController {

	@Autowired
	private StorageJobOpsQueueService jobOpsQueueService;
	
	// ------------------------------------------------------------------------
	
	@PutMapping()
	public AddJobOpsQueueResponseDTO createJobQueue(@RequestBody AddJobOpsQueueRequestDTO req) {
		val res = jobOpsQueueService.createJobQueue(req);
		return res;
	}
	
	@DeleteMapping("/{jobId}")
	public void deleteJobQueue(@PathVariable("jobId") int jobId) {
		jobOpsQueueService.deleteJobQueue(jobId);
	}
	
	@PutMapping("/add-ops-to-job-queue")
	public void addOpsToJobQueue(@RequestBody AddOpsToJobQueueRequestDTO req) {
		jobOpsQueueService.addOpsToJobQueue(req);
	}

	@GetMapping("/{jobId}/info")
	public JobQueueInfoDTO getJobQueueInfo(@PathVariable("jobId") int jobId) {
		val res = jobOpsQueueService.getJobQueueInfo(jobId);
		return res;
	}

	@GetMapping("/infos")
	public List<JobQueueInfoDTO> getJobQueueInfos() {
		val res = jobOpsQueueService.getJobQueueInfos();
		return res;
	}
	
	@GetMapping("/{jobId}/stats")
	public JobQueueStatsDTO getJobQueueStats(@PathVariable("jobId") int jobId) {
		val res = jobOpsQueueService.getJobQueueStats(jobId);
		return res;
	}
	
	@GetMapping("/stats")
	public List<JobQueueStatsDTO> getJobQueuesStats() {
		val res = jobOpsQueueService.getJobQueuesStats();
		return res;
	}

	@GetMapping("/{jobId}/remain-ops")
	public List<BlobStorageOperationDTO> listJobQueueRemainOps(@PathVariable("jobId") int jobId, 
			@RequestParam(name = "max", required = false, defaultValue = "200") int max
			) {
		val allRemainOps = jobOpsQueueService.listJobQueueRemainOps(jobId);
		val remainOps = (allRemainOps.size() > max)? allRemainOps.subList(0, max-1) : allRemainOps;
		val res = BlobStorageOperation.toDTOs(remainOps);
		return res;
	}
	
}
