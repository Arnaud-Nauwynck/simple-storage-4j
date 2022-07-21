package org.simplestorage4j.opscommon.configuration;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties(prefix = "storage-app")
@Component
@Getter @Setter
public class CommonStorageOpsAppParams {

	private List<BlobStorageParams> blobStorages;
	
	@Getter @Setter
	public static class BlobStorageParams {
		private String id;
		private String groupId;
		private String displayName;
		private String classname; // "FileBlobStorage", "AdlsGen2BlobStorage", .. 
		private String url;
		private String clientId;
		private String clientSecret;
		private Map<String,String> props;
	}

}
