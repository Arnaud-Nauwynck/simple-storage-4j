package org.simplestorage4j.s3;

import org.simplestorage4j.api.BlobStorageGroupId;
import org.simplestorage4j.api.BlobStorageId;

/**
 * adapter for BlobStorage api, delegating to S3Client
 * using fixed bucketName, and interpreting object key as path (with extra "/")
 */
public class FixedBucketS3BlobStorage extends AbstractS3BlobStorage {

    private final String bucketName;

    // --------------------------------------------------------------------------------------------

    public FixedBucketS3BlobStorage(BlobStorageId id, BlobStorageGroupId groupId, String displayName,
            S3Client s3Client, String bucketName) {
        super(id, groupId, displayName, s3Client);
        this.bucketName = bucketName;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected BucketAndKey pathToS3(String path) {
        // ensure no "/" at start
        String key;
        if (path.startsWith("/")) {
            key = path.substring(1);
        } else {
            key = path;
        }
        return new BucketAndKey(bucketName, key);
    }

    @Override
    protected String s3ToPath(String bucketName, String key) {
        return "/" + key;
    }

}
