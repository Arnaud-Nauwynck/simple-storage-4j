package org.simplestorage4j.opscommon.dto.queue;

import java.io.Serializable;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class JobQueueInfoDTO implements Serializable {
	
	/** */
	private static final long serialVersionUID = 1L;
	
	public long jobId;
	
	public long createTime;
	public String displayMessage;
	
	public Map<String,String> props;

}
