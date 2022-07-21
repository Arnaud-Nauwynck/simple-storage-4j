package org.simplestorage4j.opscommon.dto.session;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO for Executor session info
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorSessionUpdatePollingRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;

	public boolean pollingSuspendRequested;

	public boolean stopRequested;
	
	public boolean killRequested;

}
