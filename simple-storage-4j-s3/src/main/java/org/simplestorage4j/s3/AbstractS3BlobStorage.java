package org.simplestorage4j.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStorageGroupId;
import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.BlobStoreFileInfo;
import org.simplestorage4j.api.util.BlobStorageNotImpl;
import org.simplestorage4j.api.util.BlobStorageIOUtils;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.val;

/**
 * adapter for BlobStorage api, delegating to S3Client
 *
 */
public abstract class AbstractS3BlobStorage extends BlobStorage {

    private final S3Client s3Client;

    // --------------------------------------------------------------------------------------------

    public AbstractS3BlobStorage(BlobStorageId id, BlobStorageGroupId groupId, String displayName, //
            S3Client s3Client) {
        super(id, groupId, displayName);
        this.s3Client = s3Client;
    }

    // --------------------------------------------------------------------------------------------

    protected abstract BucketAndKey pathToS3(String path);

    protected abstract String s3ToPath(String bucketName, String key);

    // --------------------------------------------------------------------------------------------

    @Override
    public BlobStoreFileInfo pathInfo(String filePath) {
        val s3 = pathToS3(filePath);
        val s3ObjMetadata = s3Client.getObjectMetadata(s3.bucketName, s3.key);
        if (s3ObjMetadata == null) {
            return null;
        }
        long fileLen = s3ObjMetadata.getContentLength();
        Date lastModified = s3ObjMetadata.getLastModified();
        long lastModifiedMillis = (lastModified != null)? lastModified.getTime() : 0;
        return new BlobStoreFileInfo(filePath, false, fileLen, lastModifiedMillis);
    }

    protected ObjectMetadata s3ObjectMetadata(String filePath) {
        val s3 = pathToS3(filePath);
        return s3Client.getObjectMetadata(s3.bucketName, s3.key);
    }

    @Override
    public boolean exists(String filePath) {
        val s3ObjMetadata = s3ObjectMetadata(filePath);
        return s3ObjMetadata != null;
    }

    @Override
    public boolean isDirectory(String filePath) {
        return false;
    }

    @Override
    public long fileLen(String filePath) {
        val s3ObjMetadata = s3ObjectMetadata(filePath);
        return s3ObjMetadata.getContentLength();
    }

    @Override
    public long lastModifiedTime(String filePath) {
        val s3ObjMetadata = s3ObjectMetadata(filePath);
        Date lastModified = s3ObjMetadata.getLastModified();
        return (lastModified != null)? lastModified.getTime() : 0;
    }

    @Override
    public List<BlobStoreFileInfo> list(String filePath) {
        val s3 = pathToS3(filePath);
        ListObjectsAndCommonPrefixes tmpres = s3Client.listObjectsV2_fetchAll(s3.bucketName, s3.key, "/");
        val res = new ArrayList<BlobStoreFileInfo>();
        if (! tmpres.commonPrefixes.isEmpty()) {
            // emulate dirs
            for(val prefix: tmpres.commonPrefixes) {
                val path = s3ToPath(s3.bucketName, prefix);
                res.add(new BlobStoreFileInfo(path, true, 0, 0));
            }
        }
        if (! tmpres.objectSummmaries.isEmpty()) {
            // convert to info
            for(S3ObjectSummary s3ObjSummary: tmpres.objectSummmaries) {
                val path = s3ToPath(s3.bucketName, s3ObjSummary.getKey());
                long fileLen = s3ObjSummary.getSize();
                Date lastModified = s3ObjSummary.getLastModified();
                long lastModifiedMillis = (lastModified != null)? lastModified.getTime() : 0;
                res.add(new BlobStoreFileInfo(path, false, fileLen, lastModifiedMillis));
            }
        }
        return res;
    }

    @Override
    public InputStream openRead(String filePath, long position) {
        val s3 = pathToS3(filePath);
        val in = s3Client.openObject(s3.bucketName, s3.key);
        if (position != 0) {
            try {
        		BlobStorageIOUtils.skipFully(in, position);
            } catch (IOException ex) {
                throw new WrappedS3ClientException("Failed skip(" + position + ") on S3 object " + s3, ex, 
                        displayName, s3.bucketName, s3.key);
            }
        }
        return in;
    }

    @Override
    public byte[] readFile(String filePath) {
        val s3 = pathToS3(filePath);
        val res = s3Client.getObjectContent_readAllBytes(s3.bucketName, s3.key);
        return res;
    }

    @Override
    public byte[] readAt(String filePath, final long position, final int len) {
        val s3 = pathToS3(filePath);
        val in = s3Client.openObject(s3.bucketName, s3.key);
        if (position != 0) {
        	try {
        		BlobStorageIOUtils.skipFully(in, position);
            } catch (IOException ex) {
                throw new WrappedS3ClientException("Failed skip(" + position + ") on S3 object " + s3, ex, 
                        displayName, s3.bucketName, s3.key);
            }
        }
        try {
        	val res = BlobStorageIOUtils.readFully(in, len);
        	return res;
        } catch (IOException ex) {
            throw new WrappedS3ClientException("Failed read(" + ((position != 0)? "pos:" + position + ", ": "") + len + ") on S3 object " + s3, ex, 
                    displayName, s3.bucketName, s3.key);
        }
    }

    // Write operations
    // --------------------------------------------------------------------------------------------

    @Override
    public void mkdirs(String filePath) {
        // do nothing!.. do 'dir' in S3
    }

    @Override
    public void deleteFile(String filePath) {
        throw BlobStorageNotImpl.notImpl();
    }

    @Override
    public void renameFile(String filePath, String newFilePath) {
        throw BlobStorageNotImpl.notImpl();
    }

    @Override
    public OutputStream openWrite(String filePath, boolean append) {
        throw BlobStorageNotImpl.notImpl();
    }

    @Override
    public void writeFile(String filePath, byte[] data) {
        throw BlobStorageNotImpl.notImpl();
    }

    @Override
    public void writeAppendToFile(String filePath, byte[] appendData) {
        throw BlobStorageNotImpl.notImpl();
    }

}
