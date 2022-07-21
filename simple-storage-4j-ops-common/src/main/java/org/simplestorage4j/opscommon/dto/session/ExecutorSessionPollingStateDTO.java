package org.simplestorage4j.opscommon.dto.session;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO for Executor session polling state
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorSessionPollingStateDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;

	public long lastPingAliveTime;

	public boolean pollingSuspendRequested;
	public boolean pollingSuspended;

	public boolean stopRequested;
	public boolean stopping;
	
	public boolean killRequested;

}
