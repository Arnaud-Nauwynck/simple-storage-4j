package org.simplestorage4j.executor.configuration;

import java.util.concurrent.TimeUnit;

import org.simplestorage4j.executor.impl.StorageJobOpsExecutorCallbackClient;
import org.simplestorage4j.executor.impl.StorageJobOpsExecutorsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.val;
import okhttp3.OkHttpClient;

@Configuration
public class StorageOpsExecutorConfiguration {

	@Autowired
	private OpsExecutorAppParams opsExecutorAppParams;
	
	// @Bean
	public OkHttpClient okHttpClient() {
		val httpHeaderName = opsExecutorAppParams.getServerAuthHeaderName();
		val httpHeaderValue = opsExecutorAppParams.getServerAuthHeaderApiKey();
		OkHttpClient okHttpClient = new OkHttpClient.Builder()
				.readTimeout(5, TimeUnit.MINUTES)
				.connectTimeout(2, TimeUnit.MINUTES)
				.addInterceptor(new WithHeaderOkHttpInterceptor(httpHeaderName, httpHeaderValue))
				.build();
		return okHttpClient;
	}
	
	@Bean
	public StorageJobOpsExecutorCallbackClient opsExecutorCallbackClient() {
		val okHttpClient = okHttpClient();
		val baseServerUrl = opsExecutorAppParams.getServerUrl();
		val displayProps = opsExecutorAppParams.getDisplayProps();
		return new StorageJobOpsExecutorCallbackClient(okHttpClient, baseServerUrl, displayProps);
	}
	
	@Bean
	public StorageJobOpsExecutorsService storageJobOpsExecutorsService() {
		int opsThreadCount = opsExecutorAppParams.getOpsThreadCount();
		int opSubTasksThreadCount = opsExecutorAppParams.getOpSubTasksThreadCount();
		int opLargeFileRangeThreadCount = opsExecutorAppParams.getOpLargeFileRangeThreadCount();
		return StorageJobOpsExecutorsService.createDefault(opsThreadCount, opSubTasksThreadCount, opLargeFileRangeThreadCount);
	}

}
