package org.simplestorage4j.s3;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
public class S3ClientParams {

	private String name;

	private String endpoint;
	private String region;
	private String accessKey;
	private String secretKey;

	@Override
	public String toString() {
		return "{S3ClientParams " + name + ", '" + endpoint + "'" //
				+ ((region != null)? " region=" + region : "") //
				+ ", accessKey=" + accessKey //
				+ "}";
	}

}
