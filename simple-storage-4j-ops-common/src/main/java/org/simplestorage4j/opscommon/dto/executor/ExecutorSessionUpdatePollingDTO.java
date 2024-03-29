package org.simplestorage4j.opscommon.dto.executor;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorSessionUpdatePollingDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public boolean pollingSuspendRequested;

	public boolean stopRequested;
	
	public boolean killRequested;
	
}
