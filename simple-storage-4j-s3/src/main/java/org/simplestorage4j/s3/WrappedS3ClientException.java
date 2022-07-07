package org.simplestorage4j.s3;

public class WrappedS3ClientException extends RuntimeException {

    public final String s3ClientDisplayName;
    public final String s3BucketName;
    public final String s3Key;
    
    public WrappedS3ClientException(String message, Throwable cause,
            String s3ClientDisplayName,
            String s3BucketName,
            String s3Key
            ) {
        super(message, cause);
        this.s3ClientDisplayName = s3ClientDisplayName;
        this.s3BucketName = s3BucketName;
        this.s3Key = s3Key;
    }
    
}
