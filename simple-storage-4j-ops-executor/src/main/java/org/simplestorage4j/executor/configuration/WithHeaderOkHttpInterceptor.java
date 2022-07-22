package org.simplestorage4j.executor.configuration;

import java.io.IOException;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Interceptor.Chain;

public class WithHeaderOkHttpInterceptor implements Interceptor {

	private final String httpHeaderName;
	private final String httpHeaderValue;
	
	public WithHeaderOkHttpInterceptor(String httpHeaderName, String httpHeaderValue) {
		this.httpHeaderName = httpHeaderName;
		this.httpHeaderValue = httpHeaderValue;
	}

	@Override
	public Response intercept(Chain chain) throws IOException {
		Request newRequest = chain.request().newBuilder()
                .addHeader(httpHeaderName, httpHeaderValue)
                .build();
        return chain.proceed(newRequest);
	}
	
}