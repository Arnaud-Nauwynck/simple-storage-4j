package org.simplestorage4j.azure.datalake.configuration;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;

import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class AzureStorageFactoryUtils {

    public static final String ABFSS = "abfss://";
    public static final String DEFAULT_ENDPOINT_DOMAIN = ".dfs.core.windows.net";

    /** http endpoint: https://${accountName}.dfs.core.windows.net */
    public static String accountToHttpsEndpoint(String accountName) {
        return "https://" + accountName + DEFAULT_ENDPOINT_DOMAIN;
    }

    public static ProxyOptions proxyOptions(String proxyTypeText, String proxyHost, int proxyPort,
            String proxyUsername, String proxyPassword) {
        InetSocketAddress addr = new InetSocketAddress(proxyHost, proxyPort);
        ProxyOptions.Type proxyType = ProxyOptions.Type.valueOf(proxyTypeText);
        ProxyOptions proxyOptions = new ProxyOptions(proxyType, addr);
        if (proxyUsername != null && !proxyUsername.isEmpty()) {
            proxyOptions.setCredentials(proxyUsername, proxyPassword);
        }
        return proxyOptions;
    }

    public static void sanityCheckOkHttpClasses() {
        val cl = AzureStorageFactoryUtils.class.getClassLoader();
        log.info("com.azure:azure-core-http-okhttp:1.11.0 -> depends on com.squareup.okhttp3:okhttp:4.10.0");
        logClassResource(cl, "com.azure.core.http.okhttp.OkHttpAsyncHttpClient");
        logClassResource(cl, "okhttp3.OkHttpClient");
        //?? logClassResource(cl, "okhttp3.RequestBody");
        logClassResource(cl, "okhttp3.Authenticator");
    }

    public static void logClassResource(ClassLoader cl, String className) {
        String resource = className.replace(".", "/") + ".class";
        val url = cl.getResource(resource);
        log.info("resource for class " + className + " : " + url);
    }

    public static HttpClient azureHttpClient(ProxyOptions proxyOptions, boolean useOkHttp, boolean wrapVerboseHttpCli) {
        log.info("using az HttpClient "
                + ((proxyOptions != null)? " proxy:" + proxyOptions.getAddress().getHostName() : "")
                + ((useOkHttp)? " impl:OkHttp" : "")
                + ((wrapVerboseHttpCli)? " wrap for verbose" : "")
                );
        if (useOkHttp) {
            sanityCheckOkHttpClasses();
            return azureHttpClient_okhttp(proxyOptions, wrapVerboseHttpCli);
        } else {
            return azureHttpClient_netty(proxyOptions, wrapVerboseHttpCli);
        }
    }

    public static HttpClient azureHttpClient_okhttp(ProxyOptions proxyOptions, boolean wrapVerboseHttpCli) {
        val httpClientBuilder = new OkHttpAsyncHttpClientBuilder();

        httpClientBuilder.connectionTimeout(Duration.ofMinutes(1));
        httpClientBuilder.readTimeout(Duration.ofMinutes(5));
        httpClientBuilder.writeTimeout(Duration.ofMinutes(5));

        String displayProxyOpts = "";
        if(proxyOptions != null && proxyOptions.getType() != null) {
            displayProxyOpts = "(proxy " + proxyOptions.getType() + " " + proxyOptions.getAddress() + ") ";
            httpClientBuilder.proxy(proxyOptions);
        }
        HttpClient httpClient = httpClientBuilder.build();

        if (wrapVerboseHttpCli) {
            httpClient = new AzHttpClientWrapper(httpClient, displayProxyOpts);
        }
        return httpClient;
    }

    public static HttpClient azureHttpClient_netty(ProxyOptions proxyOptions, boolean wrapVerboseHttpCli) {
        // did not work ... bug in azure-core-http-netty > 1.0.0   ... but incompatible jar with 1.0.0 and .. 
        val httpClientBuilder = new NettyAsyncHttpClientBuilder();

        // httpClientBuilder.connectionTimeout(Duration.ofMinutes(1));
        httpClientBuilder.readTimeout(Duration.ofMinutes(5));
        httpClientBuilder.writeTimeout(Duration.ofMinutes(5));

        String displayProxyOpts = "";
        if(proxyOptions != null && proxyOptions.getType() != null) {
            displayProxyOpts = "(proxy " + proxyOptions.getType() + " " + proxyOptions.getAddress() + ") ";
            httpClientBuilder.proxy(proxyOptions);
        }
        HttpClient httpClient = httpClientBuilder.build();

        if (wrapVerboseHttpCli) {
            httpClient = new AzHttpClientWrapper(httpClient, displayProxyOpts);
        }
        return httpClient;
    }

    @Slf4j
    @RequiredArgsConstructor
    public static class AzHttpClientWrapper implements com.azure.core.http.HttpClient {
        final com.azure.core.http.HttpClient delegate;
        final String displayProxyOpts;

        @Override
        public Mono<HttpResponse> send(HttpRequest request) {
            log.info("az send http request " + request.getHttpMethod() + " " + displayProxyOpts + request.getUrl());
            return delegate.send(request);
        }

    }

    public static ClientSecretCredential createSpnCredential(HttpClient httpClient,
            String tenantId, String clientId, String clientSecret) {
        return new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .httpClient(httpClient)
                .build();
    }

    public static DataLakeServiceClientBuilder createDataLakeServiceClientBuilder(HttpClient httpClient,
            String storageAccountEndpoint, TokenCredential credential, boolean verboseAdlsGen2,
            HttpLogDetailLevel logLevel) {
        log.info("using az DataLakeServiceClient for " + storageAccountEndpoint
                // + ", cred:" + credential
                );
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .credential(credential)
                .endpoint(storageAccountEndpoint)
                .httpClient(httpClient);
        if (verboseAdlsGen2) {
            HttpLogOptions logOptions = new HttpLogOptions();
            if (logLevel == null) {
                logLevel = HttpLogDetailLevel.BASIC; // othewise NONE by default in setLogLevel()
            }
            logOptions.setLogLevel(logLevel);
            builder.httpLogOptions(logOptions);
        }
        return builder;
    }

    public static DataLakeServiceClient createDataLakeServiceClient(
            HttpClient httpClient, //
            String storageAccountEndpoint, // cf  accountToHttpsEndpoint(accountName)
            TokenCredential credential, //
            boolean verboseAdlsGen2, //
            HttpLogDetailLevel logLevel // HttpLogDetailLevel.BASIC
            ) {
        val builder = createDataLakeServiceClientBuilder(httpClient, storageAccountEndpoint,
                credential, verboseAdlsGen2, logLevel);
        return builder.buildClient();
    }

    public static DataLakeServiceAsyncClient createDataLakeServiceAsyncClient(
            HttpClient httpClient, //
            String storageAccountEndpoint, // cf  accountToHttpsEndpoint(accountName)
            TokenCredential credential, //
            boolean verboseAdlsGen2, //
            HttpLogDetailLevel logLevel // HttpLogDetailLevel.BASIC
            ) {
        val builder = createDataLakeServiceClientBuilder(httpClient, storageAccountEndpoint,
                credential, verboseAdlsGen2, logLevel);
        return builder.buildAsyncClient();
    }

}
