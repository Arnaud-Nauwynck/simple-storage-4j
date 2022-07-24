package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.List;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.opsserver.service.StorageJobOpsQueueEntry.JobQueueData;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

// @Service.. cf StorageOpsServerConfiguration
@Slf4j
public class StorageJobOpsQueueDao {

	private static final String STATE_FILENAME = "state.json";
	
	@Getter
	private final BlobStorage storage;
	@Getter
	private final String baseDir;

	// ------------------------------------------------------------------------
	
	public StorageJobOpsQueueDao(BlobStorage stateStorage, String baseDir) {
		this.storage = stateStorage;
		this.baseDir = baseDir;
	}

	// ------------------------------------------------------------------------

	private String toDirPath(JobQueueData data) {
		return toDirPath(data.jobId);
	}

	public String toDirPath(long jobId) {
		return baseDir + "/" + jobId;
	}

	public void updateJobQueueData(JobQueueData data, int way) {
		val dirPath = toDirPath(data);
		if (way == 1) {
			if (! storage.exists(dirPath)) {
				storage.mkdirs(dirPath);
			}
		}
		val filePath = dirPath + "/" + STATE_FILENAME;
		if (way >= 0) {
			storage.writeFileJson(filePath, data);
		} else {
			storage.deleteFile(filePath);
			// also delete dir?
		}
	}

	public List<JobQueueData> listJobQueueDatas() {
		val res = new ArrayList<JobQueueData>();
		val subDirs = storage.list(baseDir);
		for(val subDir: subDirs) {
			val filePath = subDir.path + "/" + STATE_FILENAME;
			if (storage.exists(filePath)) {
				try {
					val data = storage.readFileJson(filePath, JobQueueData.class);
					res.add(data);
				} catch(Exception ex ) {
					log.error("Failed to read queue data file " + filePath + ".. ignore", ex);
				}
			}
		}
		return res;
	}

}
