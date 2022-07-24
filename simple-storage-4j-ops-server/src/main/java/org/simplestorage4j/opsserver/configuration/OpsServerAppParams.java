package org.simplestorage4j.opsserver.configuration;

import java.util.List;

import org.simplestorage4j.opscommon.configuration.CommonStorageOpsAppParams.BlobStorageParams;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties(prefix = "storage-app-server")
@Component
@Getter @Setter
public class OpsServerAppParams {

	private String serverAuthHeaderName; // "api-key"
	
	private List<ApiKeyParam> authApiKeys;
	
	@Getter @Setter
	public static class ApiKeyParam {
		private String apiKey;
		private String principal;
		private List<String> authorities;
	}

	private BlobStorageParams stateStorage;
	
	private String queueStorageBaseDir = "queues";

	private String queueStatisticsStorageBaseDir = "queues-statistics";
	private String sessionStatisticsStorageBaseDir = "session-statistics";
	private String storageGroupStatisticsStorageBaseDir = "storage-group-statistics";

}
