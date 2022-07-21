package org.simplestorage4j.executor.configuration;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStorageGroupId;
import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.BlobStorageRepository;
import org.simplestorage4j.api.FileBlobStorage;
import org.simplestorage4j.api.util.BlobStorageUtils;
import org.simplestorage4j.azure.datalake.AdlsGen2BlobStorage;
import org.simplestorage4j.azure.datalake.configuration.AzureStorageFactoryUtils;
import org.simplestorage4j.executor.configuration.OpsExecutorAppParams.BlobStorageParams;
import org.simplestorage4j.s3.BucketAsFirstDirS3BlobStorage;
import org.simplestorage4j.s3.FixedBucketS3BlobStorage;
import org.simplestorage4j.s3.S3Client;
import org.simplestorage4j.s3.S3ClientParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.ProxyOptions.Type;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class OpsExecutorAppConfiguration {

	@Autowired
	private OpsExecutorAppParams appParams;

	// ------------------------------------------------------------------------
	
	@Bean
	public BlobStorageRepository blobStorageRepository() {
		val blobStorages = BlobStorageUtils.map(appParams.getBlobStorages(), params -> createBlobStorage(params));
		return new BlobStorageRepository(blobStorages); 
	}

	private BlobStorage createBlobStorage(BlobStorageParams params) {
		val className = params.getClassname();
		val idText = Objects.requireNonNull(params.getId());
		val id = BlobStorageId.of(idText);
		val groupId = BlobStorageGroupId.of(stringOrDefault(params.getGroupId(), idText));
		val displayName = stringOrDefault(params.getDisplayName(), idText);
		switch(className) {
		case "FileBlobStorage":
			return createFileBlobStorage(id, groupId, displayName, params);
		case "BucketAsFirstDirS3BlobStorage":
			return createBucketAsFirstDirS3BlobStorage(id, groupId, displayName, params);
		case "FixedBucketS3BlobStorage":
			return createFixedBucketS3BlobStorage(id, groupId, displayName, params);
		case "AdlsGen2BlobStorage":
			return createAdlsGen2BlobStorage(id, groupId, displayName, params);
		default:
			throw new IllegalArgumentException("unsupported BlobStorage classname '" + className + "'");
		}
	}

	private static String stringOrDefault(String value, String defaultValue) {
		val res = (value != null)? value : defaultValue;
		return Objects.requireNonNull(res);
	}

	private BlobStorage createFileBlobStorage(BlobStorageId id, BlobStorageGroupId groupId, String displayName, 
			BlobStorageParams params) {
		File baseDir = new File(params.getUrl());
		if (! baseDir.exists()) {
			log.info("creating dir " + baseDir + " for BlobStorage id:" + id);
			baseDir.mkdirs();
		}
		return new FileBlobStorage(id, groupId, displayName, baseDir);
	}

	private BlobStorage createBucketAsFirstDirS3BlobStorage( //
			BlobStorageId id, BlobStorageGroupId groupId, String displayName, // 
			BlobStorageParams params) {
		val props = Objects.requireNonNull(params.getProps()); 
		S3Client s3Client = createS3Client(displayName, params, props);
		
        String bucketPrefixProp = props.get("bucketPrefix");
        if (bucketPrefixProp == null) {
        	bucketPrefixProp = "";
        }
        val bucketPrefix = bucketPrefixProp;
        Function<String,String> dirToS3BucketName = (dirName) -> {
			if (dirName.startsWith(bucketPrefix)) {
				return dirName.substring(bucketPrefix.length());
			}
			return dirName; // should not occur
		};
        Function<String,String> s3BucketNameToDirName = (bucketName) -> {
        	return bucketPrefix + bucketName;
        };
		return new BucketAsFirstDirS3BlobStorage(id, groupId, displayName, //
				s3Client, dirToS3BucketName, s3BucketNameToDirName);
	}

	private BlobStorage createFixedBucketS3BlobStorage( //
			BlobStorageId id, BlobStorageGroupId groupId, String displayName, //
			BlobStorageParams params) {
		val props = Objects.requireNonNull(params.getProps()); 
		S3Client s3Client = createS3Client(displayName, params, props);
		val bucketName = Objects.requireNonNull(props.get("bucketName"));
		return new FixedBucketS3BlobStorage(id, groupId, displayName, s3Client, bucketName);
	}

	private S3Client createS3Client(String displayName, BlobStorageParams params,
			Map<String, String> props) {
		val s3Params = new S3ClientParams(displayName, 
				params.getUrl(), // endpoint
				props.get("region"),
				params.getClientId(), params.getClientSecret() //  // accessKey, secretKey
				);
		return new S3Client(displayName, s3Params);
	}	

	private BlobStorage createAdlsGen2BlobStorage( //
			BlobStorageId id, BlobStorageGroupId groupId, String displayName, //
			BlobStorageParams params) {
		val props = Objects.requireNonNull(params.getProps()); 
		boolean useOkHttp = true; // TOADD params..
		boolean wrapVerboseHttpCli = false; // TOADD params..
		
		ProxyOptions proxyOptions = null;
		val proxyHostname = props.get("proxyHost");
		if (proxyHostname != null) {
			Type type = Type.SOCKS5; // TOADD params..
			int proxyPort = 443; // TOADD params..
			InetSocketAddress address = new InetSocketAddress(proxyHostname, proxyPort); 
			proxyOptions = new ProxyOptions(type, address); 
		}
		val azureHttpClient = AzureStorageFactoryUtils.azureHttpClient(proxyOptions, useOkHttp, wrapVerboseHttpCli);
		
        val tenantId = Objects.requireNonNull(props.get("tenantId"));
        val clientId = Objects.requireNonNull(params.getClientId());
        val clientSecret = Objects.requireNonNull(params.getClientSecret());
        TokenCredential credential = AzureStorageFactoryUtils.createSpnCredential(
        		azureHttpClient, tenantId, clientId, clientSecret);

        boolean verboseAdlsGen2 = false;
        HttpLogDetailLevel logLevel = HttpLogDetailLevel.BASIC;

        // may parse abfss:// url..
        String storageAccountEndpoint = params.getUrl(); // AzureStorageFactoryUtils.accountToHttpsEndpoint(storageAccountName);
        String fileSystem = Objects.requireNonNull(props.get("fileSystem"));
        String directoryName = Objects.requireNonNull(props.get("directoryName"));
        
		val azDatalakeServiceClient = AzureStorageFactoryUtils.createDataLakeServiceClient(azureHttpClient, 
				storageAccountEndpoint, credential, verboseAdlsGen2, logLevel);
		val azDatalakeFileSystemClient = azDatalakeServiceClient.getFileSystemClient(fileSystem);
		DataLakeDirectoryClient baseDirClient = azDatalakeFileSystemClient.getDirectoryClient(directoryName);
		return new AdlsGen2BlobStorage(id, groupId, displayName, baseDirClient);
	}

}
