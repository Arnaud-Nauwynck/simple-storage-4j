package org.simplestorage4j.opscommon.dto.executor;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor
public class ExecutorSessionPollOpRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;

}
