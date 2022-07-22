package org.simplestorage4j.opsserver.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageOpsServerConfiguration {

	@Autowired
	private OpsServerAppParams opsServerAppParams;
	
}
