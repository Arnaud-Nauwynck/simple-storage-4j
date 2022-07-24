package org.simplestorage4j.api.ops.executor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.iocost.dto.QueueStatsDTO;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Persisted Queue for BlobStorage Operations
 * 
 * delegate to in-memory BlobStorageJobOperationsExecQueue + use internal hook 
 * for appending 'done' operations to <code>doneOpsFilePath</code> file
 * and refilling more in-memory ops from <code>opsFilePath</code> file
 * 
 */
@Slf4j
public class BlobStorageJobOperationsPersistedQueue {

	private final long jobId;

	private final BlobStorage storage;
	
	private final String opsFilePath;
	private final String doneOpsFilePath;
	
	/** delegate (partially) in-memory queue + hook using InnerQueueHook */
	private final BlobStorageJobOperationsExecQueue delegate;

	private long lastFlushedDoneTime;
	private long maxElapseFlushedDoneMillis = 30_000;
	private OutputStream writeDoneOutput;
	
	// ------------------------------------------------------------------------
	
	public BlobStorageJobOperationsPersistedQueue(long jobId, BlobStorage storage, String queueBaseDirPath) {
		this.jobId = jobId;
		this.storage = storage;
		this.opsFilePath = queueBaseDirPath + "/ops.txt";
		this.doneOpsFilePath = queueBaseDirPath + "/doneOps.txt";
		
		if (! storage.exists(queueBaseDirPath)) {
			storage.mkdirs(queueBaseDirPath);
		}
		val opHook = new InnerQueueHook();
		this.delegate = new BlobStorageJobOperationsExecQueue(jobId, opHook, false);
	}
	
	public void init() {
	}

	public void close() {
		if (writeDoneOutput != null) {
			try {
				writeDoneOutput.flush();
				lastFlushedDoneTime = System.currentTimeMillis();
			} catch(Exception ex) {
				log.error("Faield to flush '" + doneOpsFilePath + "' ..ignore?", ex);
			}
			try {
				writeDoneOutput.close();
			} catch(Exception ex) {
				log.error("Faield to close '" + doneOpsFilePath + "' ..ignore?", ex);
			}
			this.writeDoneOutput = null;
		}
	}
	
	// ------------------------------------------------------------------------

	protected OutputStream ensureWriteOpenDone() {
		if (writeDoneOutput == null) {
			lastFlushedDoneTime = System.currentTimeMillis();
			this.writeDoneOutput = storage.openWrite(doneOpsFilePath, true);
		}
		return this.writeDoneOutput;
	}

	/** append to done file... flush if older than 30s */
	private void appendToDoneFile(String text) {
		val out = ensureWriteOpenDone();
		try {
			out.write(text.getBytes());
		} catch (IOException ex) {
			// should not occur!!
			log.error("Failed to append to persisted queue '" + doneOpsFilePath + "' !.. ignore?", ex);
		}
		val now = System.currentTimeMillis();
		val elapsed = now - lastFlushedDoneTime;
		if (elapsed > maxElapseFlushedDoneMillis) {
			try {
				this.writeDoneOutput.flush();
				this.lastFlushedDoneTime = now;
			} catch (IOException ex) {
				log.error("Failed to flush persisted queue '" + doneOpsFilePath + "' !.. ignore?", ex);
			}
		}
	}

	// ------------------------------------------------------------------------

	public boolean hasRemainOps() {
		return delegate.hasRemainOps();
	}

	public QueueStatsDTO getQueueStatsDTO() {
		return delegate.getQueueStatsDTO();
	}

	public long newTaskIdsRange(int size) {
		return delegate.newTaskIdsRange(size);
	}

	public void addOps(List<BlobStorageOperation> ops) {
		delegate.addOps(ops);
		// TODO also append to persistent file!!
	
	}

	public BlobStorageOperation poll() {
		return delegate.poll();
	}
	
	public void onOpExecuted(BlobStorageOperationResult opResult) {
		delegate.onOpExecuted(opResult);
		// => cf hook for appending to doneFilePath
	}

	public void onOpRequeue(BlobStorageOperation op) {
		delegate.onOpRequeue(op);
	}

	public List<BlobStorageOperation> listRemainOps() {
		return delegate.listRemainOps();
	}

	public List<BlobStorageOperationError> listOpErrors() {
		return delegate.listOpErrors();
	}

	public List<BlobStorageOperationWarning> listOpWarnings() {
		return delegate.listOpWarnings();
	}

	// ------------------------------------------------------------------------

	/**
	 * inner Hook for queue operation, to persist + refill in-memory queue
	 */
	private class InnerQueueHook extends BlobStorageOperationExecQueueHook {

		public InnerQueueHook() {
		}

		@Override
		public void onOpExecutedSuccess(BlobStorageOperationResult result, BlobStorageOperation op) {
			val hasWarn = (result.warnings != null && ! result.warnings.isEmpty());
			val statusLetter = (result.errorMessage != null)? 'E' // should not occur?
					: (hasWarn)? 'W' : '-';
			val sb = new StringBuilder(100);
			sb.append(statusLetter + ":" + op.taskId 
					+ ":" + result.startTime //
					+ ":" + result.elapsedMillis
					+ ":");
			if (hasWarn) {
				sb.append(result.warnings.toString().replaceAll(":", "_"));
			}
			sb.append(":");
			// .. should not occur: errorMessage, exception;

			result.ioTimePerStorage.toTextLine(sb, '=', ',');
			sb.append("\n");
			val line = sb.toString();
			appendToDoneFile(line);
		}

		@Override
		public void onFinished() {
			// TOADD no more in-memory ops.. may refill more remaining ops from persisted file

		}
		
	}

	
}
