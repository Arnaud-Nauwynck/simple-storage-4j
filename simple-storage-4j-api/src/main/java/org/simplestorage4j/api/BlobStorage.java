package org.simplestorage4j.api;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.val;

public abstract class BlobStorage {

    public abstract BlobStoreFileInfo pathInfo(String relativePath);

	public abstract boolean exists(String filePath);

	// TODO use fileInfo()
	public abstract boolean isDirectory(String filePath);

	// TODO use fileInfo()
	public abstract long fileLen(String filePath);
	    
	// TODO use fileInfo()
	public abstract long lastModifiedTime(String filePath);


	public abstract void mkdirs(String filePath);

	public List<String> listChildNames(String filePath) {
	    return list(filePath).stream().map(x -> x.childName()).collect(Collectors.toList());
	}

	public abstract List<BlobStoreFileInfo> list(String filePath);

	public abstract void deleteFile(String filePath);

	public abstract void renameFile(String filePath, String newFilePath);


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

	public abstract byte[] readAt(String filePath, long position, int len);


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
