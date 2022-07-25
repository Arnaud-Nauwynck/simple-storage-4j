package org.simplestorage4j.api.ops.executor;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStorageRepository;
import org.simplestorage4j.api.iocost.dto.QueueStatsDTO;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.encoder.BlobStorageOpDoneLineFormatter;
import org.simplestorage4j.api.ops.encoder.BlobStorageOpErrorFormatter;
import org.simplestorage4j.api.ops.encoder.BlobStorageOpWarningFormatter;
import org.simplestorage4j.api.ops.encoder.BlobStorageOperationFormatter.BlobStorageOperationReader;
import org.simplestorage4j.api.ops.encoder.BlobStorageOperationFormatter.BlobStorageOperationWriter;
import org.simplestorage4j.api.ops.executor.BlobStorageJobOperationsExecQueue.BlobStorageOperationsQueueDTO;

import lombok.Getter;
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

	private static final String OPS_FILENAME = "ops.txt";
	private static final String DONE_FILENAME = "done-ops.txt";
	private static final String WARNING_FILENAME = "warning-ops.txt";
	private static final String ERROR_FILENAME = "error-ops.txt";

	@Getter
	private final long jobId;

	private final BlobStorage storage;
	
	private final String opsFilePath;
	private final String doneOpsFilePath;
	private final String warningOpsFilePath;
	private final String errorOpsFilePath;
	
	/** delegate (partially) in-memory queue + hook using InnerQueueHook */
	private final BlobStorageJobOperationsExecQueue delegate;

	private final BlobStorageOpDoneLineFormatter doneLineFormatter;
	private final BlobStorageOpWarningFormatter warningFormatter;
	private final BlobStorageOpErrorFormatter errorFormatter;

	private long maxElapseFlushedDoneMillis = 30_000;

	private long lastFlushedDoneTime;
	private OutputStream writeDoneOutput;
	
	private long lastFlushedWarningTime;
	private OutputStream writeWarningOutput;
	
	private long lastFlushedErrorTime;
	private OutputStream writeErrorOutput;
	
	// ------------------------------------------------------------------------
	
	public BlobStorageJobOperationsPersistedQueue(long jobId, BlobStorage storage, String queueBaseDirPath) {
		this.jobId = jobId;
		this.storage = storage;
		this.opsFilePath = queueBaseDirPath + "/" + OPS_FILENAME;
		this.doneOpsFilePath = queueBaseDirPath + "/" + DONE_FILENAME;
		this.warningOpsFilePath = queueBaseDirPath + "/" + WARNING_FILENAME;
		this.errorOpsFilePath = queueBaseDirPath + "/" + ERROR_FILENAME;
		
		if (! storage.exists(queueBaseDirPath)) {
			storage.mkdirs(queueBaseDirPath);
		}
		val opHook = new InnerQueueHook();
		this.delegate = new BlobStorageJobOperationsExecQueue(jobId, opHook, false);
		this.doneLineFormatter = new BlobStorageOpDoneLineFormatter(jobId);
		this.warningFormatter = new BlobStorageOpWarningFormatter(jobId);
		this.errorFormatter = new BlobStorageOpErrorFormatter(jobId);
	}
	
	public void setReloadedData(BlobStorageOperationsQueueDTO data, BlobStorageRepository blobStorageRepository) {
		long now = System.currentTimeMillis();
		this.lastFlushedDoneTime = now;
		this.lastFlushedWarningTime = now;
		this.lastFlushedErrorTime = now;
		// reload in-memory remaining ops from files 'ops.txt' without already done ids from 'done.txt'
		// step 1/2: load done ids
		val doneIds = new HashSet<Long>();
		val doneResults = new ArrayList<BlobStorageOperationResult>();
		if (storage.exists(doneOpsFilePath)) {
			try(val in = storage.openRead(doneOpsFilePath)) {
				val lineReader = new BufferedReader(new InputStreamReader(in));
				for(;;) {
					val line = lineReader.readLine();
					if (line == null) {
						break;
					}
					if (line.startsWith("#") || line.isEmpty()) {
						continue;
					}
					val opResult = this.doneLineFormatter.parseLineFromDoneFile(line);
					doneIds.add(opResult.taskId);
					doneResults.add(opResult);
				}
			} catch(IOException ex) { 
				val msg = "Failed to read " + doneOpsFilePath;
				throw new RuntimeException(msg, ex);
			}
		}

		this.delegate.setReloadedData(data, doneResults);

		// step 2/2: load ops, retained not already done
		val ops = new ArrayList<BlobStorageOperation>();
		if (storage.exists(opsFilePath)) {
			try (val in = storage.openRead(opsFilePath)) {
				val lineReader = new BufferedReader(new InputStreamReader(in));
				val opReader = new BlobStorageOperationReader(jobId, lineReader, blobStorageRepository);
				for(;;) {
					val op = opReader.read();
					if (op == null) {
						break;
					}
					if (! doneIds.contains(op.taskId)) {
						ops.add(op);
					}
				}
			} catch(IOException ex) { 
				val msg = "Failed to read " + opsFilePath;
				throw new RuntimeException(msg, ex);
			}
		}

		delegate.reevalQueuePreEstimateIOCosts();
		delegate.addOps(ops);
		
	}

	public void close() {
		if (writeDoneOutput != null) {
			try {
				writeDoneOutput.flush();
				lastFlushedDoneTime = System.currentTimeMillis();
			} catch(Exception ex) {
				log.error("Failed to flush '" + doneOpsFilePath + "' ..ignore?", ex);
			}
			try {
				writeDoneOutput.close();
			} catch(Exception ex) {
				log.error("Failed to close '" + doneOpsFilePath + "' ..ignore?", ex);
			}
			this.writeDoneOutput = null;
		}

		if (writeWarningOutput != null) {
			try {
				writeWarningOutput.flush();
				lastFlushedWarningTime = System.currentTimeMillis();
			} catch(Exception ex) {
				log.error("Failed to flush '" + warningOpsFilePath + "' ..ignore?", ex);
			}
			try {
				writeWarningOutput.close();
			} catch(Exception ex) {
				log.error("Failed to close '" + warningOpsFilePath + "' ..ignore?", ex);
			}
			this.writeWarningOutput = null;
		}

		if (writeErrorOutput != null) {
			try {
				writeErrorOutput.flush();
				lastFlushedErrorTime = System.currentTimeMillis();
			} catch(Exception ex) {
				log.error("Failed to flush '" + errorOpsFilePath + "' ..ignore?", ex);
			}
			try {
				writeErrorOutput.close();
			} catch(Exception ex) {
				log.error("Failed to close '" + errorOpsFilePath + "' ..ignore?", ex);
			}
			this.writeErrorOutput = null;
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
		flushOldFiles();
	}

	// ------------------------------------------------------------------------

	protected OutputStream ensureWriteOpenWarnings() {
		if (writeWarningOutput == null) {
			lastFlushedWarningTime = System.currentTimeMillis();
			this.writeWarningOutput = storage.openWrite(warningOpsFilePath, true);
		}
		return this.writeWarningOutput;
	}
	
	/** append to warning file... flush if older than 30s */
	private void appendToWarningFile(String text) {
		val out = ensureWriteOpenWarnings();
		try {
			out.write(text.getBytes());
		} catch (IOException ex) {
			// should not occur!!
			log.error("Failed to append to persisted queue '" + warningOpsFilePath + "' !.. ignore?", ex);
		}
		flushOldFiles();
	}


	// ------------------------------------------------------------------------

	protected OutputStream ensureWriteOpenError() {
		if (writeErrorOutput == null) {
			lastFlushedErrorTime = System.currentTimeMillis();
			this.writeErrorOutput = storage.openWrite(errorOpsFilePath, true);
		}
		return this.writeDoneOutput;
	}
	
	/** append to done file... flush if older than 30s */
	private void appendToErrorFile(String text) {
		val out = ensureWriteOpenError();
		try {
			out.write(text.getBytes());
		} catch (IOException ex) {
			// should not occur!!
			log.error("Failed to append to persisted queue '" + errorOpsFilePath + "' !.. ignore?", ex);
		}
		flushOldFiles();
	}


	// ------------------------------------------------------------------------
	
	private void flushOldFiles() {
		val now = System.currentTimeMillis();
		val elapsed = now - lastFlushedDoneTime;
		if (elapsed > maxElapseFlushedDoneMillis) {
			doFlushFiles(now);
		}
	}

	private void doFlushFiles(long now) {
		if (writeDoneOutput != null) {
			try {
				this.writeDoneOutput.flush();
				this.lastFlushedDoneTime = now;
			} catch (IOException ex) {
				log.error("Failed to flush persisted queue '" + doneOpsFilePath + "' !.. ignore?", ex);
			}
		}
		if (writeWarningOutput != null) {
			try {
				this.writeWarningOutput.flush();
				this.lastFlushedWarningTime = now;
			} catch (IOException ex) {
				log.error("Failed to flush persisted queue '" + warningOpsFilePath + "' !.. ignore?", ex);
			}
		}
		if (writeErrorOutput != null) {
			try {
				this.writeErrorOutput.flush();
				this.lastFlushedErrorTime = now;
			} catch (IOException ex) {
				log.error("Failed to flush persisted queue '" + errorOpsFilePath + "' !.. ignore?", ex);
			}
		}
	}

	public void periodicCheckFlushFiles() {
		flushOldFiles();
	}

	public void flushFiles() {
		doFlushFiles(System.currentTimeMillis());
	}

	// ------------------------------------------------------------------------

	public boolean hasRemainOps() {
		return delegate.hasRemainOps();
	}

	public QueueStatsDTO toQueueStatsDTO() {
		return delegate.toQueueStatsDTO();
	}

	public long newTaskIdsRange(int size) {
		return delegate.newTaskIdsRange(size);
	}

	public void addOps(List<BlobStorageOperation> ops) {
		delegate.addOps(ops);
		// append to persistent file
		try(val out = storage.openWrite(opsFilePath, true)) {
			val printOut = new PrintWriter(new BufferedOutputStream(out));
			val opWriter = new BlobStorageOperationWriter(printOut);
			opWriter.write(ops);
			printOut.flush();
		} catch(IOException ex) {
			throw new RuntimeException("Failed to write ops to '" + opsFilePath + "'", ex);
		}
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

	public BlobStorageOperationsQueueDTO toQueueDTO() {
		return delegate.toDTO();
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
			val line = doneLineFormatter.formatLineToDoneFile(result);
			appendToDoneFile(line);
			
			if (result.warnings != null && !result.warnings.isEmpty()) {
				val warnText = warningFormatter.format(result);
				appendToWarningFile(warnText);
			}
		}

		@Override
		public void onOpExecutedError(BlobStorageOperationResult result, BlobStorageOperation op) {
			if (result.warnings != null && !result.warnings.isEmpty()) {
				// ??
				val warnText = warningFormatter.format(result);
				appendToWarningFile(warnText);
			}
			val errorText = errorFormatter.format(result, op);
			appendToErrorFile(errorText);
		}

		@Override
		public void onFinished() {
			flushFiles();
			// TOADD no more in-memory ops.. may refill more remaining ops from persisted file
			
		}
		
	}

	
}
