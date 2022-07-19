package org.simplestorage4j.api;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.ops.dto.BlobStoragePathDTO;

/**
 * immutable wrapper for {BlobStorage, String path}
 */
public class BlobStoragePath {

	public final @Nonnull BlobStorage blobStorage;
	public final @Nonnull String path;
	
	// ------------------------------------------------------------------------
	
	public BlobStoragePath(@Nonnull BlobStorage blobStorage, @Nonnull String path) {
		this.blobStorage = Objects.requireNonNull(blobStorage);
		this.path = Objects.requireNonNull(path);
	}

	// ------------------------------------------------------------------------
	
	public BlobStoragePathDTO toDTO() {
		return new BlobStoragePathDTO(blobStorage.id.id, path);
	}
	
	public BlobStoreFileInfo pathInfo() {
		return blobStorage.pathInfo(path);
	}

	public boolean exists() {
		return blobStorage.exists(path);
	}

	public boolean isDirectory() {
		return blobStorage.isDirectory(path);
	}

	public long fileLen() {
		return blobStorage.fileLen(path);
	}
	    
	public long lastModifiedTime() {
		return blobStorage.lastModifiedTime(path);
	}

	public void mkdirs() {
		blobStorage.mkdirs(path);
	}

	public List<String> listChildNames() {
		return blobStorage.listChildNames(path);
	}

	public List<BlobStoreFileInfo> list() {
		return blobStorage.list(path);
	}

	public void deleteFile() {
		blobStorage.deleteFile(path);
	}

	public void renameFile(String newFilePath) {
		blobStorage.renameFile(path, newFilePath);
	}

	public OutputStream openWrite() {
		return blobStorage.openWrite(path);
	}

	public OutputStream openWrite(boolean append) {
		return blobStorage.openWrite(path, append);
	}
	
	public InputStream openRead() {
		return blobStorage.openRead(path, 0);
	}

	public InputStream openRead(long position) {
		return blobStorage.openRead(path, position);
	}

	
	// only [int].. 2Go supported here
	public void writeFile(byte[] data) {
		blobStorage.writeFile(path, data);
	}

	public void writeAppendToFile(byte[] appendData) {
		blobStorage.writeAppendToFile(path, appendData);
	}

	// only [int].. 2Go supported here
	public byte[] readFile() {
		return blobStorage.readFile(path);
	}

	public byte[] readAt(long position, int len) {
		return blobStorage.readAt(path, position, len);
	}

	// Json helper
	// ------------------------------------------------------------------------
	
	public <T> void writeFileJson(T data) {
		blobStorage.writeFileJson(path, data);
	}

	public <T> T readFileJson(Class<T> type) {
		return blobStorage.readFileJson(path, type);
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return blobStorage.displayName + " '" + path + "'";
	}
	
}
