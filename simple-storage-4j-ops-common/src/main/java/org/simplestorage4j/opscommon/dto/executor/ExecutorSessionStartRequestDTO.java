package org.simplestorage4j.opscommon.dto.executor;

import java.io.Serializable;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorSessionStartRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId; // generated on client-side, unique host + id (ex: startTime)
	public String host;
	public long startTime;

	public Map<String,String> props;

}
