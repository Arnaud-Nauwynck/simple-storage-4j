package org.simplestorage4j.executor.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.util.LoggingCounter;
import org.simplestorage4j.api.util.LoggingCounter.LoggingCounterParams;
import org.simplestorage4j.opscommon.dto.executor.ExecutorOpFinishedPollNextRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorOpsFinishedRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPingAliveRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpsResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStartRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStopRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionUpdatePollingDTO;

import lombok.val;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.PUT;

/**
 * Retrofit2 client corresponding to {@Link org.simplestorage4j.opsserver.rest.StorageOpsExecutorCallbackRestController}
 */
@Slf4j
public class StorageJobOpsExecutorCallbackClient {

	private String sessionId; // generated on client-side, at init
	private String hostname;
	private long startTime;
	private Map<String,String> props;

	private StorageJobOpsExecutorCallbackRetrofit2Interface delegate;

	private final LoggingCounter loggingCounter_call = new LoggingCounter("executorClient call", new LoggingCounterParams(1, 0));
	private final LoggingCounter loggingCounter_pingAlive = new LoggingCounter("executorClient pingAlive", new LoggingCounterParams(100, 600_000));
	private final LoggingCounter loggingCounter_pollOp = new LoggingCounter("executorClient pollOp", new LoggingCounterParams(100, 600_000));
	private final LoggingCounter loggingCounter_onOpsFinished = new LoggingCounter("executorClient onOpsFinished", new LoggingCounterParams(100, 600_000));
	private final LoggingCounter loggingCounter_onOpFinishedPollNext = new LoggingCounter("executorClient onOpFinishedPollNext", new LoggingCounterParams(100, 600_000));
	private final LoggingCounter loggingCounter_onOpsFinishedPollNexts = new LoggingCounter("executorClient onOpsFinishedPollNexts", new LoggingCounterParams(100, 600_000));

	// ------------------------------------------------------------------------

	public StorageJobOpsExecutorCallbackClient(OkHttpClient okHttpClient, String baseServerUrl, Map<String,String> props) {
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException ex) {
			log.error("should not occur: failed to get local hostname.. using default", ex);
			this.hostname = "localhost?";
		}
		this.props = props;

		Retrofit retrofit = new Retrofit.Builder()
			    .baseUrl(baseServerUrl)
			    .client(okHttpClient)
			    .addConverterFactory(JacksonConverterFactory.create())
		        // .addConverterFactory(ScalarsConverterFactory.create())
			    .build();

		this.delegate = retrofit.create(StorageJobOpsExecutorCallbackRetrofit2Interface.class);
	}

	// ------------------------------------------------------------------------

	private static interface StorageJobOpsExecutorCallbackRetrofit2Interface {

		public static final String BASE_PATH = "api/storage-ops/executor-callback";

		@PUT(BASE_PATH + "/onExecutorStart")
		public Call<Void> onExecutorStart(@Body ExecutorSessionStartRequestDTO req);

		@PUT(BASE_PATH + "/onExecutorStop")
		public Call<Void> onExecutorStop(@Body ExecutorSessionStopRequestDTO req);

		@PUT(BASE_PATH + "/onExecutorPingAlive")
		public Call<ExecutorSessionUpdatePollingDTO> onExecutorPingAlive(@Body ExecutorSessionPingAliveRequestDTO req);

		@PUT(BASE_PATH + "/poll-op")
		public Call<ExecutorSessionPollOpResponseDTO> pollOp(@Body ExecutorSessionPollOpRequestDTO req);

		@PUT(BASE_PATH + "/on-ops-finished")
		public Call<Void> onOpsFinished(@Body ExecutorOpsFinishedRequestDTO req);

		@PUT(BASE_PATH + "/on-op-finished-poll-next")
		public Call<ExecutorSessionPollOpResponseDTO> onOpFinishedPollNext(@Body ExecutorOpFinishedPollNextRequestDTO req);

		@PUT(BASE_PATH + "/on-ops-finished-poll-nexts")
		public Call<ExecutorSessionPollOpsResponseDTO> onOpsFinishedPollNexts(@Body ExecutorOpsFinishedRequestDTO req);

	}

	// ------------------------------------------------------------------------

	public boolean isSessionStarted() {
		return this.sessionId != null;
	}
	
	public void onExecutorStart() {
		if (this.sessionId == null) {
			log.info("session already started!"); // may re-send to relaunched server?
			return;
		}
		this.startTime = System.currentTimeMillis();
		val sessionId = hostname + ":" + startTime;

		val req = new ExecutorSessionStartRequestDTO(sessionId, hostname, startTime, props);
		execHttp(loggingCounter_call, "PUT", "onExecutorStart", "", //
				delegate.onExecutorStart(req));
		this.sessionId = sessionId;
	}

	public void onExecutorStop(String stopReason) {
		val sessionId = this.sessionId;
		if (sessionId != null) {
			val req = new ExecutorSessionStopRequestDTO(sessionId, stopReason);
			execHttp(loggingCounter_call, "PUT", "onExecutorStop", "", //
					delegate.onExecutorStop(req));
			this.sessionId = null;
		}
	}

	public ExecutorSessionUpdatePollingDTO onExecutorPingAlive() {
		val req = new ExecutorSessionPingAliveRequestDTO(sessionId);
		return execHttp(loggingCounter_pingAlive, "PUT", "onExecutorPingAlive", "", //
				delegate.onExecutorPingAlive(req));
	}

	public ExecutorSessionPollOpResponseDTO pollOp() {
		val req = new ExecutorSessionPollOpRequestDTO(sessionId);
		return execHttp(loggingCounter_pollOp, "PUT", "pollOp", "", //
				delegate.pollOp(req));
	}

	public void onOpsFinished(List<BlobStorageOperationResult> opResults) {
		val opResultDtos = BlobStorageOperationResult.toDtos(opResults);
		val req = new ExecutorOpsFinishedRequestDTO(sessionId, opResultDtos, 0);
		execHttp(loggingCounter_onOpsFinished, "PUT", "onOpsFinished", "", //
				delegate.onOpsFinished(req));
	}

	public ExecutorSessionPollOpResponseDTO onOpFinishedPollNext(BlobStorageOperationResult opResult) {
		val opResultDto = opResult.toDTO();
		val req = new ExecutorOpFinishedPollNextRequestDTO(sessionId, opResultDto);
		return execHttp(loggingCounter_onOpFinishedPollNext, "PUT", "onOpFinishedPollNext", "", //
				delegate.onOpFinishedPollNext(req));
	}

	public ExecutorSessionPollOpsResponseDTO onOpsFinishedPollNexts(List<BlobStorageOperationResult> opResults, int pollCount) {
		val opResultDtos = BlobStorageOperationResult.toDtos(opResults);
		val req = new ExecutorOpsFinishedRequestDTO(sessionId, opResultDtos, pollCount);
		return execHttp(loggingCounter_onOpsFinishedPollNexts, "PUT", "onOpsFinishedPollNext", "", //
				delegate.onOpsFinishedPollNexts(req));
	}

	// ------------------------------------------------------------------------

	protected <T> T execHttp(
			LoggingCounter loggingCounter,
			String httpMethod, String httpRelativePath, String displayMessage,
			Call<T> call) {
		val startTime = System.currentTimeMillis();
		Response<T> resp;
		try {
			resp = call.execute();
		} catch (IOException ex) {
			String errorMsg = "Failed to call http " + httpMethod + " " + httpRelativePath;
			log.error(errorMsg, ex);
			throw new RuntimeException(errorMsg, ex);
		}
		if (! resp.isSuccessful()) {
			val errorCode = resp.code();
			String errorBodyText;
			try {
				ResponseBody errorBody = resp.errorBody();
				if (errorBody != null) {
					errorBodyText = errorBody.string();
				} else {
					errorBodyText = "";
				}
			} catch (IOException e) {
				errorBodyText = "(failed to get error body: " + e.getMessage() + ")";
			}
			throw new RuntimeException("Failed http " + httpMethod + " " + httpRelativePath + ": " + errorCode + " " + errorBodyText);
		}

		val res = resp.body();

		val millis = System.currentTimeMillis() - startTime;
		loggingCounter.incr(millis, logPrefix -> log.info(logPrefix + " http " + httpMethod + " " + httpRelativePath));
		return res;
	}

}
