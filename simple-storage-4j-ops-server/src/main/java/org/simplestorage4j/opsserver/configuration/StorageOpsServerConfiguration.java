package org.simplestorage4j.opsserver.configuration;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.opscommon.configuration.CommonStorageOpsAppConfiguration;
import org.simplestorage4j.opsserver.service.StorageJobOpsQueueDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageOpsServerConfiguration {

	@Autowired
	private OpsServerAppParams opsServerAppParams;
	
	@Bean
	public BlobStorage stateStorage() {
		return CommonStorageOpsAppConfiguration.createBlobStorage(opsServerAppParams.getStateStorage());
	}

	@Bean
	public StorageJobOpsQueueDao storageJobOpsQueueDao(
			BlobStorage stateStorage) {
		return new StorageJobOpsQueueDao(stateStorage, 
				opsServerAppParams.getQueueStorageBaseDir());
	}
}
