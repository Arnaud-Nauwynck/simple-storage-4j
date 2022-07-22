package org.simplestorage4j.executor.configuration;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties(prefix = "storage-app-executor")
@Component
@Getter @Setter
public class OpsExecutorAppParams {

	private String serverUrl; // example: "http://localhost:8080"

	private String serverAuthHeaderName;
	private String serverAuthHeaderApiKey;

	private Map<String,String> displayProps;
	
	private int opsThreadCount = 10;
	private int opsPollAheadCount = 5;
	private int opSubTasksThreadCount = 20;
	private int opLargeFileRangeThreadCount = 10;
	
	private int maxPingAliveSeconds = 30;
	
}
