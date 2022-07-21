package org.simplestorage4j.opscommon.dto.session;

import java.io.Serializable;

import org.simplestorage4j.api.iocost.dto.PerBlobStoragesIOTimeResultDTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO for Executor session stats
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorSessionRecentIOStatsDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;

	public PerBlobStoragesIOTimeResultDTO totalIOTimePerStorage;
	
	// TOADD
//	public PerBlobStoragesIOTimeResultDTO[] recentRddIOTimePerStorage;
	
}
