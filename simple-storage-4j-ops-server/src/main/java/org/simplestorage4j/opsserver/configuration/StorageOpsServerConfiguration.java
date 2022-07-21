package org.simplestorage4j.opsserver.configuration;

import org.simplestorage4j.api.BlobStorageRepository;
import org.simplestorage4j.api.ops.encoder.BlobStorageOperationDtoResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageOpsServerConfiguration {

	@Bean
	public BlobStorageOperationDtoResolver blobStorageOperationDtoResolver(
			BlobStorageRepository blobStorageRepository) {
		return new BlobStorageOperationDtoResolver(blobStorageRepository);
	}
}
