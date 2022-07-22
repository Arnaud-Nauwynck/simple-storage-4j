package org.simplestorage4j.executor.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.BlobStorageOperationExecContext;
import org.simplestorage4j.api.ops.encoder.BlobStorageOperationDtoResolver;
import org.simplestorage4j.executor.configuration.OpsExecutorAppParams;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpsResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionUpdatePollingDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class StorageJobOpsExecutorPingAndPoller {

	@Autowired
	protected OpsExecutorAppParams opExecutorAppParams;
	
	@Autowired
	protected StorageJobOpsExecutorCallbackClient callbackClient;

	@Autowired
	protected StorageJobOpsExecutorsService storageJobOpsExecutorsService;
	
	@Autowired
	protected BlobStorageOperationDtoResolver blobStorageOperationDtoResolver;
	
	private final Object lock = new Object();
	
	@GuardedBy("lock")
	private int currSubmittedOpsCount;

	@GuardedBy("lock")
	private int currProcessingOpsCount;
	
	@GuardedBy("lock")
	private final List<BlobStorageOperationResult> currOpResults = new ArrayList<>();

	@GuardedBy("lock")
	private boolean currPollingSuspended = false;

	private BlobStorageOperationExecContext opExecCtx;
	
	private int currMaxSubmitCount;
	private long maxPingAliveMillis;
	
	// ------------------------------------------------------------------------
	
	@PostConstruct
	public void onInit() {
		this.currMaxSubmitCount = opExecutorAppParams.getOpsThreadCount() + opExecutorAppParams.getOpsPollAheadCount();
		this.maxPingAliveMillis = opExecutorAppParams.getMaxPingAliveSeconds() * 1000;
		if (this.maxPingAliveMillis < 5000) {
			this.maxPingAliveMillis = 5000;
		}
	}

	@PreDestroy
	public void onExit() {
		log.info("Stopping app..");
		if (callbackClient.isSessionStarted()) {
			callbackClient.onExecutorStop("onExit");
		}
	}

	public void runMainLoop() {
		log.info("call callbackClient.onExecutorStart ..");
		callbackClient.onExecutorStart();

		
		this.opExecCtx = new BlobStorageOperationExecContext(
				storageJobOpsExecutorsService.getOpsSubTasksExecutorService(),
				storageJobOpsExecutorsService.getOpsLargeFileRangeExecutorService());
		
		val startPollingTime = System.currentTimeMillis();

		long lastPingAliveTime = System.currentTimeMillis();
		
		try {
			for(;;) {
				int pollCount;
				val opResultsToSend = new ArrayList<BlobStorageOperationResult>();
				synchronized (lock) {
					pollCount = doGetCurrOpResultsAndPollCount(opResultsToSend, currPollingSuspended);
				}
				if (opResultsToSend.isEmpty() && pollCount == 0) {
					// nothing to do => wait until change or ping alive is needed
					val now = System.currentTimeMillis();
					val nextPingAliveTime = lastPingAliveTime + maxPingAliveMillis;
					val waitPingAliveMillis = nextPingAliveTime - now;
					if (waitPingAliveMillis > 0) {
						try {
							synchronized (lock) {
								lock.wait(waitPingAliveMillis);
							}
						} catch (InterruptedException e) {
						}
						// reeval if something to do after wait/notifyed
						synchronized (lock) {
							pollCount = doGetCurrOpResultsAndPollCount(opResultsToSend, currPollingSuspended);
						}
						if (opResultsToSend.isEmpty() && pollCount == 0) {
							// nothing to send neither to poll... need pingAlive
							// *** http call: ping alive ***
							val pingRes = safePingAlive();
							
							if (pingRes != null) {
								boolean stopRequested = handleUpdatePolling(pingRes);
								if (stopRequested) {
									break;
								}
								lastPingAliveTime = System.currentTimeMillis();
							}
							continue;
						}
					}
				}

				// *** the Biggy: http call submit op results + poll next ops ***
				val opsFinishedPollResp = safeOpsFinishedPollNexts(opResultsToSend, pollCount);
				
				if (opsFinishedPollResp == null) {
					// server failed to respond?.. sleep and retry later
					sleep(30_000);
					continue;
				}
				lastPingAliveTime = System.currentTimeMillis();
				if (opsFinishedPollResp.pollingResp != null) {
					boolean stopRequested = handleUpdatePolling(opsFinishedPollResp.pollingResp);
					if (stopRequested) {
						break;
					}
				}

				val opDtos = opsFinishedPollResp.ops;
				if (opDtos != null && ! opDtos.isEmpty()) {
					val ops = blobStorageOperationDtoResolver.dtosToOps(opDtos);
					// *** the biggy: submit op to execute to thread pool ***
					submitOpTasks(ops);
				} else {
					// nothing polled => sleep until ping alive needed
					val now = System.currentTimeMillis();
					val nextPingAliveTime = lastPingAliveTime + maxPingAliveMillis;
					val waitPingAliveMillis = nextPingAliveTime - now;
					if (waitPingAliveMillis > 0) {
						try {
							synchronized (lock) {
								lock.wait(waitPingAliveMillis);
							}
						} catch (InterruptedException e) {
						}
					}
				}

			} // end for(;;)
			
			waitCurrentOpsFinished();
			
			val totalSec = (System.currentTimeMillis() - startPollingTime) / 1000;
			log.info(".. done polling, took total " + totalSec + " s");

		} finally {
			log.info("call callbackClient.onExecutorStop ..");
			callbackClient.onExecutorStop("end of main loop");
		}
	}

	private boolean handleUpdatePolling(ExecutorSessionUpdatePollingDTO pollingResp) {
		boolean stopRequested = false;
		if (pollingResp.pollingSuspendRequested) {
			log.info("polling response: poll suspended requested");
			synchronized (lock) {
				currPollingSuspended = true;
				lock.notify();
			}
		} else if (currPollingSuspended) {
			log.info("polling response: poll resume requested");
			synchronized (lock) {
				currPollingSuspended = false;
				lock.notify();
			}
		}
		if (pollingResp.stopRequested) {
			log.info("polling response: stop requested");
			stopRequested = true; // => break main loop
		}
		if (pollingResp.killRequested) {
			onKillRequested();
		}
		return stopRequested;
	}

	private ExecutorSessionUpdatePollingDTO safePingAlive() {
		try {
			val res = callbackClient.onExecutorPingAlive();
			return res;
		} catch(Exception ex) {
			log.warn("Failed onExecutorPingAlive ..ignore, no rethrow (retry later) " + ex.getMessage());
			return null;
		}
	}

	private int doGetCurrOpResultsAndPollCount(List<BlobStorageOperationResult> opResultsToSend, boolean currPollingSuspended) {
		opResultsToSend.addAll(currOpResults);
		currOpResults.clear();
		int pollCount;
		int currOpsCount = currSubmittedOpsCount + currProcessingOpsCount;
		int targetOpsCount = (! currPollingSuspended)? currMaxSubmitCount : 0;
		pollCount = targetOpsCount - currOpsCount;
		if (pollCount < 0) {
			pollCount = 0;
		}
		return pollCount;
	}

	private ExecutorSessionPollOpsResponseDTO safeOpsFinishedPollNexts(
			List<BlobStorageOperationResult> opResults, int pollCount
			) {
		try {
			val res = callbackClient.onOpsFinishedPollNexts(opResults, pollCount);
			return res;
		} catch(Exception ex) {
			synchronized (lock) {
				currOpResults.addAll(opResults);
			}
			log.warn("Failed onOpsFinishedPollNexts ..ignore, no rethrow (retry later) " + ex.getMessage());
			return null;
		}
	}

	private void waitCurrentOpsFinished() {
		synchronized(lock) {
			log.info("wait until current ops are finished, currently "
				+ currSubmittedOpsCount + " submitted "
				+ " + " + currProcessingOpsCount + " processing ops");
		}
		val waitStartTime = System.currentTimeMillis();

		for(;;) {
			try {
				synchronized(lock) {
					lock.wait(1000); // wait 1s or notify
					if (currSubmittedOpsCount == 0 && currProcessingOpsCount == 0) {
						break;
					}
				}
			} catch (InterruptedException e) {
			}
		}
		val waitSec = (System.currentTimeMillis() - waitStartTime ) / 1000;
		log.info(".. done wait ops finished, took " + waitSec + " s");
	}

	private void onKillRequested() {
		log.info("polling response: kill requested => call onExecutorStop + exit");
		try {
			callbackClient.onExecutorStop("kill requested");
		} catch(Exception ex) {
			log.error("Failed onExecutorStop .. ignore", ex);
		}
		System.out.println("(stdout) exiting(-2)");
		System.out.println("(stderr) exiting(-2)");
		System.exit(-2);
	}

	private static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}

	private void submitOpTasks(Collection<BlobStorageOperation> ops) {
		synchronized (lock) {
			currSubmittedOpsCount += ops.size();
		}
		for(val op: ops) {
			this.storageJobOpsExecutorsService.getOpsExecutorService().submit(() -> doOpTask(op));
		}
	}
	
	private void doOpTask(BlobStorageOperation op) {
		synchronized (lock) {
			currSubmittedOpsCount--;
			currProcessingOpsCount++;
		}
		try {
			BlobStorageOperationResult opResult = op.execute(opExecCtx);
			
			// buffer result to queue
			synchronized (lock) {
				currOpResults.add(opResult);
			}
		} finally {
			synchronized (lock) {
				currProcessingOpsCount--;
				lock.notify();
			}
		}
	}
	
}
