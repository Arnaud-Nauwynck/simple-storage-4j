package org.simplestorage4j.api;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.simplestorage4j.api.util.BlobStorageUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.val;

public abstract class BlobStorage {

	public final BlobStorageId id;
	
	public final BlobStorageGroupId groupId;
	
	public final String displayName;
	
	// ------------------------------------------------------------------------
	
    public BlobStorage(BlobStorageId id, BlobStorageGroupId groupId, String displayName) {
		this.id = id;
		this.groupId = groupId;
		this.displayName = displayName;
	}

    // ------------------------------------------------------------------------
	
	public abstract BlobStoreFileInfo pathInfo(String filePath);

	public abstract boolean exists(String filePath);

	/** helper for <code>pathInfo(filePath).isDir</code> */
	public boolean isDirectory(String filePath) {
		val info = pathInfo(filePath);
		return info.isDir;
	}

	/** helper for <code>pathInfo(filePath).fileLength</code> */
	public long fileLen(String filePath) {
		val info = pathInfo(filePath);
		return info.fileLength;
	}
	
	/** helper for <code>pathInfo(filePath).lastModifTime</code> */
	public long lastModifiedTime(String filePath) {
		val info = pathInfo(filePath);
		return info.lastModifTime;
	}

	public abstract void mkdirs(String filePath);

	public List<String> listChildNames(String filePath) {
	    val fileInfos = list(filePath);
		return BlobStorageUtils.map(fileInfos, x -> x.childName());
	}

	public abstract List<BlobStoreFileInfo> list(String filePath);

	public abstract void deleteFile(String filePath);

	public abstract void renameFile(String filePath, String newFilePath);

	public OutputStream openWrite(String filePath) {
		return openWrite(filePath, false);
	}

	public abstract OutputStream openWrite(String filePath, boolean append);
	
	public InputStream openRead(String filePath) {
		return openRead(filePath, 0);
	}

	public abstract InputStream openRead(String filePath, long position);


	
	// only [int].. 2Go supported here
	public abstract void writeFile(String filePath, byte[] data);

	public abstract void writeAppendToFile(String filePath, byte[] appendData);


	// only [int].. 2Go supported here
	public abstract byte[] readFile(String filePath);

	public final byte[] readAt(String filePath, long position, int len) {
		byte[] res = new byte[len];
		readAt(res, 0, filePath, position, len);
		return res;
	}
	
	public abstract void readAt(byte[] resBuffer, int resPos, String filePath, long position, int len);


	// Json helper
	// ------------------------------------------------------------------------
	
	private ObjectMapper jsonMapper = createObjectMapper();

    private ObjectMapper createObjectMapper() {
        val res = new ObjectMapper();
        res.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return res;
    }
	
	public <T> void writeFileJson(String filePath, T data) {
		try(val out = new BufferedOutputStream(openWrite(filePath, false))) {
			jsonMapper.writeValue(out, data);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to write as json to file '" + filePath + "'", ex);
		}
	}

	public <T> T readFileJson(String filePath, Class<T> type) {
		try(val in = new BufferedInputStream(openRead(filePath))) {
			return (T) jsonMapper.readValue(in, type);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read as json from file '" + filePath + "'", ex);
		}
	}

}
