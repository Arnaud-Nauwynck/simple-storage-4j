package org.simplestorage4j.opscommon.dto.session;

import java.io.Serializable;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO for Executor session info
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorSessionInfoDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;
	public String host;
	public long startTime;

	public Map<String,String> props;

}
