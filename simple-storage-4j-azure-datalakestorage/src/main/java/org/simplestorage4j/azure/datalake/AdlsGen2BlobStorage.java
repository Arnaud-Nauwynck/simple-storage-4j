package org.simplestorage4j.azure.datalake;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.simplestorage4j.api.BlobStorage;
import org.simplestorage4j.api.BlobStorageGroupId;
import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.BlobStoreFileInfo;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdlsGen2BlobStorage extends BlobStorage {

    private final DataLakeDirectoryClient baseDirClient;
    private final String azFileSystem;
    private final String azBaseDirPath; // = baseDirClient.getDirectoryPath();

    @Getter
    @Setter
    protected boolean verboseWriteLog = false;
    @Getter
    @Setter
    protected int verboseWriteLogMaxMillis = 1000;

    // --------------------------------------------------------------------------------------------

    /** create using
     * DataLakeFileSystemClient fsClient = datalakeServiceClient.getFileSystemClient(filesystem);
     * .. = fsClient.getDirectoryClient(subDirPath)
     */
    public AdlsGen2BlobStorage(
            BlobStorageId id, BlobStorageGroupId groupId, String displayName, //
            DataLakeDirectoryClient baseDirClient) {
        super(id, groupId, displayName);
        this.baseDirClient = baseDirClient;
        this.azFileSystem = baseDirClient.getFileSystemName();
        this.azBaseDirPath = baseDirClient.getDirectoryPath();
    }

    // implements api BlobStorage
    // --------------------------------------------------------------------------------------------

    @Override
    public BlobStoreFileInfo pathInfo(String relativePath) {
        val child = dirClientOf(relativePath); // DirClient or FileClient both works
        if (! child.exists()) {
            return null;
        }
        PathProperties props = child.getProperties();
        String name = child.getDirectoryName();
        return toBlobStoreFileInfo(name, props);
    }

    @Override
    public boolean exists(String relativePath) {
        val dirClient = dirClientOf(relativePath); // DirClient or FileClient both works for exists()
        return dirClient.exists();
    }

    @Override
    public boolean isDirectory(String relativeFilePath) {
        val props = azQueryPathProperties(relativeFilePath);
        return props.isDirectory();
    }

    @Override
    public long fileLen(String relativeFilePath) {
        val props = azQueryPathProperties(relativeFilePath);
        return props.getFileSize();
    }

    @Override
    public long lastModifiedTime(String relativeFilePath) {
        val props = azQueryPathProperties(relativeFilePath);
        return props.getLastModified().toInstant().toEpochMilli();
    }

    @Override
    public List<BlobStoreFileInfo> list(String relativePath) {
        val dirClient = dirClientOf(relativePath);
        PagedIterable<PathItem> pagedIterable = dirClient.listPaths();
        val res = pagedIterable.stream().map(item -> toBlobStoreFileInfo(item)).collect(Collectors.toList());
        // val res = BlobStorageUtils.map(pagedIterable, item -> toBlobStoreFileInfo(item));
        return res;
    }

    @Override
    public void mkdirs(String relativePath) {
        String[] pathElts = relativePath.split("/");
        val len = pathElts.length;
        DataLakeDirectoryClient[] dirClients = new DataLakeDirectoryClient[len];
        {
            DataLakeDirectoryClient dirClient = baseDirClient;
            for(int i = 0; i < len; i++) {
                dirClient = dirClient.getSubdirectoryClient(pathElts[i]);
                dirClients[i] = dirClient;
            }
        }
        val lastDir = dirClients[len-1];
        if (! lastDir.exists()) {
            try {
                // heuristic: try to create last dir, hoping intermediate dirs already exist
                log.info("az dir.create " + lastDir.getDirectoryUrl());
                lastDir.create();
            } catch(Exception ex) {
                // Failed... slow path: check+create all intermediate dirs
                for(val dirClient: dirClients) {
                    if (! dirClient.exists()) {
                        log.info("az dir.create " + dirClient.getDirectoryUrl());
                        dirClient.create();
                    }
                }
            }
        }
    }

    @Override
    public void deleteFile(String relativePath) {
        val fileClient = fileClientOf(relativePath);
        if (! fileClient.exists()) {
            return;
        }
        PathProperties props = fileClient.getProperties();
        if (! props.isDirectory()) {
            log.info("az DataLakeFileClient.delete() " + fileClient.getFileUrl());
            fileClient.delete();
        } else {
            val dirClient = dirClientOf(relativePath);
            log.info("az DataLakeDirectoryClient.delete() " + dirClient.getDirectoryUrl());
            dirClient.delete();
        }
    }

    @Override
    public void renameFile(String relativeFilePath, String newFilePath) {
        val fileClient = fileClientOf(relativeFilePath);
        val azDestPath = toAzPath(newFilePath);
        fileClient.rename(azFileSystem, azDestPath);
    }

    @Override
    public OutputStream openWrite(String relativeFilePath, boolean append) {
        val fileClient = fileClientOf(relativeFilePath);
        boolean existed = fileClient.exists();
        if (! append) {
            if (! existed) {
                fileClient.create();
            } else {
                fileClient.create(true);
            }
            return new AzFileExclusiveOutputStream(fileClient, 0);
        } else {
            // TODO TOCHECK
            if (! existed) {
                fileClient.create();
            } else {
                // fileClient.create(false); // append ???
            }
            // fileClient.create(false);
            val props = fileClient.getProperties();
            val len = props.getFileSize();
            return new AzFileExclusiveOutputStream(fileClient, len);
        }
    }

    @Override
    public InputStream openRead(String relativeFilePath, long position) {
        val fileClient = fileClientOf(relativeFilePath);
        DataLakeFileOpenInputStreamResult inResult;
        if (position == 0) {
            inResult = fileClient.openInputStream();
        } else {
            DataLakeFileInputStreamOptions inOptions = new DataLakeFileInputStreamOptions();
            inOptions.setRange(new FileRange(position));
            inResult = fileClient.openInputStream(inOptions);
        }
        return inResult.getInputStream();
    }

    @Override
    public void writeFile(String relativeFilePath, byte[] data) {
        long startTime = System.currentTimeMillis();
        val resolved = fileAndParentDirOf(relativeFilePath);
//        if (!resolved.parentDirClient.exists()) {
//            resolved.parentDirClient.create();
//        }
        val input = new ByteArrayInputStream(data);
        try {
            resolved.fileClient.upload(input, data.length, true);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to write StorageFile '" + relativeFilePath + "'", ex);
        }
        long millis = System.currentTimeMillis() - startTime;
        if (millis > 10000) {
            logWrite("write to '" + relativeFilePath + "' length:" + data.length, millis);
        }
    }

    @Override
    public void writeAppendToFile(String relativeFilePath, byte[] appendData) {
        long startTime = System.currentTimeMillis();
        val resolved = fileAndParentDirOf(relativeFilePath);
        if (!resolved.parentDirClient.exists()) {
            resolved.parentDirClient.create();
        }
        val input = new ByteArrayInputStream(appendData);
        long fileLenRes;
        try {
            if (!resolved.fileClient.exists()) {
                resolved.fileClient.create(); // ??
                resolved.fileClient.upload(input, appendData.length, true);
                fileLenRes = appendData.length;
            } else {
                resolved.fileClient.upload(input, appendData.length, false);
                fileLenRes = resolved.fileClient.getProperties().getFileSize();
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to append to StorageFile '" + relativeFilePath + "'", ex);
        }
        long millis = System.currentTimeMillis() - startTime;
        logWrite("writeAppend to '" + relativeFilePath + "' append.length:" + appendData.length + " => " + fileLenRes, millis);
        // return fileLenRes;
    }

    @Override
    public byte[] readFile(String relativeFilePath) {
        long startTime = System.currentTimeMillis();
        val fileClient = fileClientOf(relativeFilePath);
        val buffer = new ByteArrayOutputStream();
        try {
            fileClient.read(buffer);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to read StorageFile '" + relativeFilePath + "'", ex);
        }
        val data = relativeFilePath.getBytes();
        long millis = System.currentTimeMillis() - startTime;
        log.info("read '" + relativeFilePath + "' => length:" + data.length + " .. took " + millis + " ms");
        return data;
    }

    @Override
    public void readAt(final byte[] resBuffer, final int resPos, 
    		String relativeFilePath, long position, int len) {
        long startTime = System.currentTimeMillis();
        val fileClient = fileClientOf(relativeFilePath);
        DataLakeFileInputStreamOptions inOptions = new DataLakeFileInputStreamOptions();
        inOptions.setRange(new FileRange(position, (long)len));
        DataLakeFileOpenInputStreamResult inputStreamResult = fileClient.openInputStream(inOptions);
        try (val in = inputStreamResult.getInputStream()) {
        	int currResPos = resPos;
        	int remainLen = len;
            while (remainLen > 0) {
                int count = in.read(resBuffer, currResPos, remainLen);
                if (count < 0) {
                    throw new EOFException(); // should not occur
                }
                currResPos += count;
                remainLen -= count;
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to read StorageFile '" + relativeFilePath + "'", ex);
        }
        long millis = System.currentTimeMillis() - startTime;
        log.info("readAt '" + relativeFilePath + "', " + position + ", len:" + len + " .. took " + millis + " ms");
    }

    // internal
    // --------------------------------------------------------------------------------------------

    private String toAzPath(String relativePath) {
        if (relativePath.isEmpty() || relativePath.equals("/")) {
            return azBaseDirPath;
        }
        if (relativePath.startsWith("/")) {
            relativePath = relativePath.substring(1);
        }
        return azBaseDirPath + "/" + relativePath;
    }

    private DataLakeDirectoryClient dirClientOf(String relativePath) {
        if (relativePath.isEmpty() || relativePath.equals("/")) {
            return baseDirClient;
        }
        // TODO use directly dirClient.getSubdirectoryClient(relativePath) ??
        String[] pathElts = relativePath.split("/");
        val res = chilDirClientUpTo(baseDirClient, pathElts, pathElts.length);
        return res;
    }

    private PathProperties azQueryPathProperties(String relativePath) {
        val child = dirClientOf(relativePath); // DirClient or FileClient both works
        if (! child.exists()) {
            return null;
        }
        PathProperties res = child.getProperties();
        return res;
    }

    private DataLakeDirectoryClient chilDirClientUpTo(DataLakeDirectoryClient base, String[] pathElts, int len) {
        DataLakeDirectoryClient dirClient = base;
        for(int i = 0; i < len; i++) {
            dirClient = dirClient.getSubdirectoryClient(pathElts[i]);
        }
        return dirClient;
    }

    private DataLakeFileClient fileClientOf(String relativePath) {
        String[] pathElts = relativePath.split("/");
        val parentDir = chilDirClientUpTo(baseDirClient, pathElts, pathElts.length - 1);
        return parentDir.getFileClient(pathElts[pathElts.length - 1]);
    }

    @AllArgsConstructor
    private static class FileAndParentDirClient {
        public final DataLakeDirectoryClient parentDirClient;
        public final DataLakeFileClient fileClient;
    }

    FileAndParentDirClient fileAndParentDirOf(String relativePath) {
        String[] pathElts = relativePath.split("/");
        val parentDirClient = chilDirClientUpTo(baseDirClient, pathElts, pathElts.length - 1);
        val fileClient = parentDirClient.getFileClient(pathElts[pathElts.length - 1]);
        return new FileAndParentDirClient(parentDirClient, fileClient);
    }

    protected BlobStoreFileInfo toBlobStoreFileInfo(PathItem item) {
        String name = item.getName();
        boolean isDir = item.isDirectory();
        long fileLen = item.getContentLength();
        long lastModified = item.getLastModified().toEpochSecond() * 1000;
        return new BlobStoreFileInfo(name, isDir, fileLen, lastModified);
    }

    protected BlobStoreFileInfo toBlobStoreFileInfo(String name, PathProperties item) {
        boolean isDir = item.isDirectory();
        long fileLen = item.getFileSize();
        long lastModified = item.getLastModified().toEpochSecond() * 1000;
        return new BlobStoreFileInfo(name, isDir, fileLen, lastModified);
    }




    private void logWrite(String msg, long millis) {
        msg += " .. took " + millis + " ms";
        if (verboseWriteLog || millis > verboseWriteLogMaxMillis) {
            log.info(msg);
        } else {
            log.debug(msg);
        }
    }

}
