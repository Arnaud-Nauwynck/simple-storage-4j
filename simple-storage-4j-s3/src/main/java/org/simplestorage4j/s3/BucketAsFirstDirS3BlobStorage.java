package org.simplestorage4j.s3;

import org.simplestorage4j.api.BlobStorageGroupId;
import org.simplestorage4j.api.BlobStorageId;

import java.util.function.Function;

import lombok.val;

/**
 * adapter for BlobStorage api, delegating to S3Client
 * interpreting the first path name as the bucketName
 */
public class BucketAsFirstDirS3BlobStorage extends AbstractS3BlobStorage {

    private final Function<String,String> dirToS3BucketName;
    private final Function<String,String> s3BucketNameToDirName;

    // --------------------------------------------------------------------------------------------

    public BucketAsFirstDirS3BlobStorage(BlobStorageId id, BlobStorageGroupId groupId, String displayName,
            S3Client s3Client,
            Function<String,String> dirToS3BucketName,
            Function<String,String> s3BucketNameToDirName) {
        super(id, groupId, displayName, s3Client);
        this.dirToS3BucketName = dirToS3BucketName;
        this.s3BucketNameToDirName = s3BucketNameToDirName;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected BucketAndKey pathToS3(String path) {
        // remove "/" at start
        val startsWithSlash = path.charAt(0) == '/';
        val firstIdx = (startsWithSlash)? 1 : 0;
        val sep = path.indexOf("/", firstIdx+1);
        if (sep == -1) {
            val dirName = path.substring(firstIdx);
            val bucketName = dirToS3BucketName.apply(dirName);
            return new BucketAndKey(bucketName, "/");
        }
        val dirName = path.substring(firstIdx, sep);
        val bucketName = dirToS3BucketName.apply(dirName);
        val key = path.substring(sep + 1, path.length()); // should not start with "/" ??
        return new BucketAndKey(bucketName, key);
    }

    @Override
    protected String s3ToPath(String bucketName, String key) {
        val dirName = s3BucketNameToDirName.apply(bucketName);
        return "/" + dirName + "/" + key;
    }

}
