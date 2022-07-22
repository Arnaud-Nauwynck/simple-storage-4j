package org.simplestorage4j.opsserver.configuration;

import java.util.List;

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

}
