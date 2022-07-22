package org.simplestorage4j.opsserver.configuration;

import org.simplestorage4j.api.BlobStorageRepository;
import org.simplestorage4j.api.ops.encoder.BlobStorageOperationDtoResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageOpsServerConfiguration {

	@Autowired
	private OpsServerAppParams opsServerAppParams;
	
	@Bean
	public BlobStorageOperationDtoResolver blobStorageOperationDtoResolver(
			BlobStorageRepository blobStorageRepository) {
		return new BlobStorageOperationDtoResolver(blobStorageRepository);
	}
}
