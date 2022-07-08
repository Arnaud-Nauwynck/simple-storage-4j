package org.simplestorage4j.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.simplestorage4j.api.util.LoggingCounter;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * S3Client: facade for AWS S3 apis
 */
@Slf4j
public class S3Client {

    private static final int DEFAULT_BUFFER_READ_SIZE = 16*1024;

    private final String displayName;
    
    @Getter
    private AmazonS3 s3Client;

    @Getter
    private S3ClientParams s3Properties;

    private int maxRetry = 5;

    protected final LoggingCounter counter_listBuckets = new LoggingCounter("s3 listBuckets");
    protected final LoggingCounter counter_listBuckets_Failed = new LoggingCounter("s3 listBuckets Failed");

    protected final LoggingCounter counter_doesBucketExistV2 = new LoggingCounter("s3 doesBucketExistV2");
    protected final LoggingCounter counter_doesBucketExistV2_Failed = new LoggingCounter("s3 doesBucketExistV2 Failed");

    protected final LoggingCounter counter_listObjectsV2 = new LoggingCounter("s3 listObjectsV2");
    protected final LoggingCounter counter_listObjectsV2_FailedRetry = new LoggingCounter("s3 listObjectsV2 Failed Retry");
    protected final LoggingCounter counter_listObjectsV2_Failed = new LoggingCounter("s3 listObjectsV2 Failed");

    protected final LoggingCounter counter_listObjectsV2_fetchAll = new LoggingCounter("s3 listObjectsV2_fetchAll");
    protected final LoggingCounter counter_listObjectsV2_fetchAll_Failed = new LoggingCounter("s3 listObjectsV2_fetchAll Failed");

    protected final LoggingCounter counter_getObjectMetadata = new LoggingCounter("s3 getObjectMetadata");
    protected final LoggingCounter counter_getObjectMetadata_Failed = new LoggingCounter("s3 getObjectMetadata Failed");

    protected final LoggingCounter counter_getObjectContent_readAllBytes = new LoggingCounter("s3 getObjectContent_readAllBytes");
    protected final LoggingCounter counter_getObjectContent_readAllBytes_Failed = new LoggingCounter("s3 getObjectContent_readAllBytes Failed");

    protected final LoggingCounter counter_getObjectContent_stream = new LoggingCounter("s3 getObjectContent_stream");
    protected final LoggingCounter counter_getObjectContent_stream_Failed = new LoggingCounter("s3 getObjectContent_stream Failed");

    private int requestTimeoutMillis = 10 * 60 * 1000; // 10mn

    // --------------------------------------------------------------------------------------------

    public S3Client(String displayName, S3ClientParams s3Params) {
        this.displayName = displayName;
        this.s3Properties = Objects.requireNonNull(s3Params);
        val credentials = new BasicAWSCredentials(s3Params.getAccessKey(), s3Params.getSecretKey());
        
        val clientConfig = new ClientConfiguration();
        // clientConfig.red
        // clientConfig.setRequestTimeout(requestTimeout);
        val httpClientConfig = clientConfig.getApacheHttpClientConfig();
        // httpClientConfig.getSslSocketFactory().

        this.s3Client = AmazonS3ClientBuilder
            .standard()
            .withClientConfiguration(clientConfig)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Params.getEndpoint(), s3Params.getRegion()))
            .build();
        
        setLoggingCountersFreq(100_000);
    }

    public void close() {
        if (this.s3Client != null) {
            this.s3Client.shutdown();
            this.s3Client = null;
        }
    }

    // --------------------------------------------------------------------------------------------

    public void setLoggingCountersFreq(int freq) {
        counter_listObjectsV2_fetchAll.setLogFreq(freq);
        counter_getObjectMetadata.setLogFreq(freq);
        counter_getObjectContent_readAllBytes.setLogFreq(freq);
        counter_getObjectContent_stream.setLogFreq(freq);
    }

    /**
     * facade for <code>s3Client.listBuckets()</code>
     */
    public List<Bucket> listBuckets() {
        long startTime = System.currentTimeMillis();
        try {
            val req = new ListBucketsRequest();
            req.setSdkRequestTimeout(requestTimeoutMillis);

            val res = this.s3Client.listBuckets(req);
            
            long millis = System.currentTimeMillis() - startTime;
            counter_listBuckets.incr(millis, logPrefix -> log.info(logPrefix));
            return res;
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_listBuckets_Failed.incr(millis, logPrefix -> log.error(logPrefix + " Failed " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed counter_listBuckets()", ex, displayName, null, null);
        }
    }

    /**
     * facade for <code>s3Client.doesBucketExistsV2(bucketName)</code>
     */
    public boolean doesBucketExistV2(String bucketName) {
        long startTime = System.currentTimeMillis();
        try {
            // internally equivalent to "getBucketAcl(bucketName)"
            val res = this.s3Client.doesBucketExistV2(bucketName);
            
            long millis = System.currentTimeMillis() - startTime;
            counter_doesBucketExistV2.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ")"));
            return res;
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_doesBucketExistV2_Failed.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ") Failed " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed counter_doesBucketExistV2(" + bucketName + ")", ex, displayName, bucketName, null);
        }
    }

    /**
     * facade for <code>s3Client.listObjectsV2(req)</code>, with retries
     */
    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request req) {
        val bucketName = req.getBucketName();
        ListObjectsV2Result res = null;
        for(int retry = 0; retry < maxRetry; retry++) {
            long startTime = System.currentTimeMillis();
            try {                
                res = this.s3Client.listObjectsV2(req);
                
                val millis = System.currentTimeMillis() - startTime;
                counter_listObjectsV2.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ")"));
                break;
            } catch(RuntimeException ex) {
                val millis = System.currentTimeMillis() - startTime;
                if (retry + 1 < maxRetry) {
                    counter_listObjectsV2_FailedRetry.incr(millis, logPrefix -> log.error(logPrefix + "(" + bucketName + ") retry.. ex:" + ex.getMessage()));
                    try {
                        Thread.sleep(100 + 500*retry);
                    } catch (InterruptedException e) {
                    }
                } else {
                    counter_listObjectsV2_Failed.incr(millis, logPrefix -> log.error(logPrefix + "(" + bucketName + ") rethrow after " + maxRetry + " retries .. ex:" + ex.getMessage()));
                    throw ex;
                }
            }
        }
        return res;
    }

    
    /**
     * helper for <code>listObjectsV2_fetchAll(bucketName, prefix, "/")</code>
     */
    public ListObjectsAndCommonPrefixes listObjectsV2_fetchAll(String bucketName, String prefix) {
        return listObjectsV2_fetchAll(bucketName, prefix, "/"); 
    }
    
    /**
     * facade for <code>s3Client.listObjectsV2(bucketName, prefix, delimiter)</code>
     */
    public ListObjectsAndCommonPrefixes listObjectsV2_fetchAll(String bucketName, String prefix, String delimiter) {
        long startTime = System.currentTimeMillis();
        val msgParam = "(" + bucketName + ((prefix != null)? ", prefix:" + prefix : "") + ")";
        try {
            val objectSummmaries = new ArrayList<S3ObjectSummary>(100);
            val commonPrefixes = new ArrayList<String>(100);

            ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withDelimiter(delimiter)
                .withPrefix(prefix);
            req.setSdkRequestTimeout(requestTimeoutMillis);
            
            ListObjectsV2Result result;
            do {
                result = listObjectsV2(req);
                
                objectSummmaries.addAll(result.getObjectSummaries());
                commonPrefixes.addAll(result.getCommonPrefixes());
                
                // If there are more than maxKeys (1000 by default) keys in the bucket, get a continuation token
                // and list the next objects.
                String token = result.getNextContinuationToken();
                req.setContinuationToken(token);
            } while (result.isTruncated());
                
            long millis = System.currentTimeMillis() - startTime;
            counter_listObjectsV2_fetchAll.incr(millis, logPrefix -> log.info(logPrefix + msgParam));
            return new ListObjectsAndCommonPrefixes(objectSummmaries, commonPrefixes);
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_listObjectsV2_fetchAll_Failed.incr(millis, logPrefix -> log.error(logPrefix + msgParam + " Failed (" + bucketName + ", prefix: " + prefix + ") " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed listObjectsV2_fetchAll" + msgParam, ex, displayName, bucketName, null);
        }
    }

    /**
     * facade for <code>s3Client.listObjectsV2(bucketName)</code>
     */
    public ListObjectsAndCommonPrefixes listObjectsV2_fetchAll(String bucketName) {
        long startTime = System.currentTimeMillis();
        try {
            val objectSummmaries = new ArrayList<S3ObjectSummary>(100);
            val commonPrefixes = new ArrayList<String>(100);

            ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName);
            req.setSdkRequestTimeout(requestTimeoutMillis);
            ListObjectsV2Result result;
            do {
                result = listObjectsV2(req);
                
                objectSummmaries.addAll(result.getObjectSummaries());
                commonPrefixes.addAll(result.getCommonPrefixes());
                
                // If there are more than maxKeys (1000 by default) keys in the bucket, get a continuation token
                // and list the next objects.
                String token = result.getNextContinuationToken();
                req.setContinuationToken(token);
            } while (result.isTruncated());
                
            long millis = System.currentTimeMillis() - startTime;
            counter_listObjectsV2_fetchAll.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ")"));
            return new ListObjectsAndCommonPrefixes(objectSummmaries, commonPrefixes);
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_listObjectsV2_fetchAll_Failed.incr(millis, logPrefix -> log.error(logPrefix + "(" + bucketName + ") Failed " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed listObjectsV2_fetchAll(" + bucketName + ")", ex, displayName, bucketName, null);
        }
    }

    /**
     * facade for <code>s3Client.listObjectsV2(bucketName)</code>
     */
    public void listObjectsV2_fetchAll(String bucketName, Consumer<ListObjectsAndCommonPrefixes> callback) {
        long startTime = System.currentTimeMillis();
        try {
            ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName);
            req.setSdkRequestTimeout(requestTimeoutMillis);
            ListObjectsV2Result result;
            do {
                result = listObjectsV2(req);

                val tmpres = new ListObjectsAndCommonPrefixes(result.getObjectSummaries(), result.getCommonPrefixes());
                callback.accept(tmpres);
                
                // If there are more than maxKeys (1000 by default) keys in the bucket, get a continuation token
                // and list the next objects.
                String token = result.getNextContinuationToken();
                req.setContinuationToken(token);
            } while (result.isTruncated());
                
            long millis = System.currentTimeMillis() - startTime;
            counter_listObjectsV2_fetchAll.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ")"));
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_listObjectsV2_fetchAll_Failed.incr(millis, logPrefix -> log.error(logPrefix + "(" + bucketName + ") Failed " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed listObjectsV2_fetchAll(" + bucketName + ")", ex, displayName, bucketName, null);
        }
    }
    
    /**
     * facade for <code>s3Client.getObjectMetadata(bucketName, key)</code>
     * returned null instead of throwing AmazonS3Exception 404
     */
    public ObjectMetadata getObjectMetadata(String bucketName, String key) {
        long startTime = System.currentTimeMillis();
        
        try {
            GetObjectMetadataRequest req = new GetObjectMetadataRequest(bucketName, key);
            req.setSdkRequestTimeout(requestTimeoutMillis);

            ObjectMetadata res = this.s3Client.getObjectMetadata(req);
            
            long millis = System.currentTimeMillis() - startTime;
            counter_getObjectMetadata.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key));
            return res;

        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                long millis = System.currentTimeMillis() - startTime;
                counter_getObjectMetadata.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key));
                return null; // OK, object does not exist, return null
            } else {
                long millis = System.currentTimeMillis() - startTime;
                counter_getObjectMetadata_Failed.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key + ") Failed " + ex.getMessage()));
                throw new WrappedS3ClientException("Failed counter_getObjectMetadata(" + bucketName + ", " + key + ")", ex, displayName, bucketName, key);
            }
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_getObjectMetadata_Failed.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key + ") Failed " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed counter_getObjectMetadata(" + bucketName + ", " + key + ")", ex, displayName, bucketName, key);
        }
    }
    
    /**
     * facade for <code>s3Client.getObject(bucketName, key).getObjectContent().read( buffer ) .. repeated until 0 read</code>
     */
    public byte[] getObjectContent_readAllBytes(String bucketName, String key) {
        long startTime = System.currentTimeMillis();
        try {
            GetObjectRequest req = new GetObjectRequest(bucketName, key);
            req.setSdkRequestTimeout(requestTimeoutMillis);
            S3Object s3Object = this.s3Client.getObject(req);
            val resBuffer = new ByteArrayOutputStream(20*1024);
            try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
            	byte[] readBuf = new byte[DEFAULT_BUFFER_READ_SIZE];
                int readLen;
                while ((readLen = s3ObjectInputStream.read(readBuf)) > 0) {
                    resBuffer.write(readBuf, 0, readLen);
                }
            } catch (IOException ex) {
                throw new WrappedS3ClientException("Failed S3 getObjectBytes(" + bucketName + ", " + key + ")", ex, displayName, bucketName, key);
            }
            
            long millis = System.currentTimeMillis() - startTime;
            counter_getObjectContent_readAllBytes.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key));
            return resBuffer.toByteArray();
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_getObjectContent_readAllBytes_Failed.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key + ") Failed " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed getObjectContent_readAllBytes(" + bucketName + ", " + key + ")", ex, displayName, bucketName, key);
        }
    }

    /**
     * facade for <code>s3Client.getObject(bucketName, key).getObjectContent()</code>
     */
    public InputStream openObject(String bucketName, String key) {
        long startTime = System.currentTimeMillis();
        try {
            GetObjectRequest req = new GetObjectRequest(bucketName, key);
            req.setSdkRequestTimeout(requestTimeoutMillis);
            S3Object object = s3Client.getObject(req);
            val res = object.getObjectContent();
            long millis = System.currentTimeMillis() - startTime;
            counter_getObjectContent_stream.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key + ")"));
            return res;
        } catch(Exception ex) {
            long millis = System.currentTimeMillis() - startTime;
            counter_getObjectContent_stream_Failed.incr(millis, logPrefix -> log.info(logPrefix + "(" + bucketName + ", " + key + ") Failed " + ex.getMessage()));
            throw new WrappedS3ClientException("Failed getObjectContent_stream(" + bucketName + ", " + key + ")", ex, displayName, bucketName, key);
        }
    }

}
