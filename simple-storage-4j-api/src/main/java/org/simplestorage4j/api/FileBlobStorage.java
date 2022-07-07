package org.simplestorage4j.api;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileBlobStorage extends BlobStorage {

	public final File baseDir;
	
	// ------------------------------------------------------------------------
	
	public FileBlobStorage(
			BlobStorageId id, BlobStorageGroupId groupId, String displayName, // 
			File baseDir) {
		super(id, groupId, displayName);
		this.baseDir = baseDir;
	}

	// ------------------------------------------------------------------------
	
	protected File toFile(String subFilePath) {
		return new File(baseDir, subFilePath);
	}
	
	@Override
    public BlobStoreFileInfo pathInfo(String relativePath) {
	    val file = toFile(relativePath);
	    return toBlobStoreFileInfo(relativePath, file);
	}

    private BlobStoreFileInfo toBlobStoreFileInfo(String relativePath, File file) {
        return new BlobStoreFileInfo(relativePath, file.isDirectory(), file.length(), file.lastModified());
    }

	@Override
	public boolean exists(String filePath) {
		val file = toFile(filePath);
		return file.exists();
	}

	@Override
	public List<BlobStoreFileInfo> list(String relativePath) {
        log.info("ls " + displayName + " '" + relativePath + "'");
        val file = toFile(relativePath);
        val tmpres = file.listFiles();
        val res = new ArrayList<BlobStoreFileInfo>();
		for (val e : tmpres) {
            String childRelativePath = relativePath + "/" + e.getName();
            res.add(toBlobStoreFileInfo(childRelativePath, e));
        }
        return res;
	}

	@Override
    public List<String> listChildNames(String filePath) {
        log.info("ls " + displayName + " '" + filePath + "'");
        val file = toFile(filePath);
        val tmpres = file.list();
        return new ArrayList<>(Arrays.asList(tmpres));
    }

	@Override
	public void mkdirs(String filePath) {
		log.info("mkdirs " + displayName + " '" + filePath + "'");
		val file = toFile(filePath);
		file.mkdirs();
	}
	
	@Override
	public void deleteFile(String filePath) {
		log.info("delete " + displayName + " file '" + filePath + "'");
		val file = toFile(filePath);
		file.delete();
	}

	@Override
	public void renameFile(String filePath, String newFilePath) {
		log.info("rename " + displayName + " file '" + filePath + "' -> '" + newFilePath + "'");
		val file = toFile(filePath);
		val newFile = toFile(newFilePath);
		file.renameTo(newFile);
	}

	@Override
	public boolean isDirectory(String filePath) {
		val file = toFile(filePath);
		return file.isDirectory();
	}

	@Override
	public long fileLen(String filePath) {
		val file = toFile(filePath);
		return file.length();
	}
	
	@Override
	public long lastModifiedTime(String filePath) {
		val file = toFile(filePath);
		return file.lastModified();
	}

	
	@Override
	public OutputStream openWrite(String filePath, boolean append) {
		log.info("open write" + ((append)? "-append" : "") + " to " + displayName + " file '" + filePath + "'");
		val file = toFile(filePath);
		try {
			return new FileOutputStream(file, append);
		} catch(IOException ex) {
			throw new RuntimeException("Failed to open write to file '" + filePath + "'", ex);
		}
	}

	@Override
	public InputStream openRead(String filePath) {
		log.info("open read from " + displayName + " file '" + filePath + "'");
		val file = toFile(filePath);
		try {
			return new FileInputStream(file);
		} catch(IOException ex) {
			throw new RuntimeException("Failed to open read from file '" + filePath + "'", ex);
		}
	}

	@Override
	public InputStream openRead(String filePath, long position) {
		log.info("open read from " + displayName + " file '" + filePath + "' pos:" + position);
		val file = toFile(filePath);
		try {
			val res = new FileInputStream(file);
			skipFully(res, position);
			return res;
		} catch(IOException ex) {
			throw new RuntimeException("Failed to open read from file '" + filePath + "' pos:" + position, ex);
		}
	}

	@Override
	public void writeFile(String filePath, byte[] data) {
		log.info("write to " + displayName + " file '" + filePath + "'");
		val file = toFile(filePath);
		try (val out = new FileOutputStream(file)) {
			out.write(data);
		} catch(IOException ex) {
			throw new RuntimeException("Failed to write to file '" + filePath + "'", ex);
		}
	}

	@Override
	public void writeAppendToFile(String filePath, byte[] appendData) {
	    // too verbose..
		// log.info("write append to " + displayName + " file '" + filePath + "'");
		val file = toFile(filePath);
		try (val out = new FileOutputStream(file, true)) {
			out.write(appendData);
		} catch(IOException ex) {
			throw new RuntimeException("Failed to write append to file '" + filePath + "'", ex);
		}
	}

	@Override
	public byte[] readFile(String filePath) {
		log.info("read " + displayName + " file '" + filePath + "'");
		val file = toFile(filePath);
		long lenLong = file.length(); // only 2Go supported here
		if (lenLong > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException();
		}
		int len = (int) lenLong;
		try (val in = new FileInputStream(file)) {
			// readFully
			byte[] res = readFully(in, len);
			return res;
		} catch(IOException ex) {
			throw new RuntimeException("Failed to read file '" + filePath + "'", ex);
		}
	}

	@Override
	public byte[] readAt(String filePath, long position, int len) {
		log.info("read " + displayName + " file '" + filePath + "'");
		val file = toFile(filePath);
		long lenLong = file.length(); // only 2Go supported here
		if (lenLong > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException();
		}
		try (val in = new FileInputStream(file)) {
			if (position != 0) {
				skipFully(in, position);
			}
			byte[] res = readFully(in, len);
			return res;
		} catch(IOException ex) {
			throw new RuntimeException("Failed to read file '" + filePath + "'", ex);
		}
	}

	// ------------------------------------------------------------------------
	
	private static void skipFully(InputStream in, final long len) throws IOException {
		int nSkip = 0;
		while(nSkip < len) {
			long count = in.skip(len - nSkip);
			if (count < 0) {
				throw new EOFException();
			}
			nSkip += count;
		}
	}

	private static byte[] readFully(FileInputStream in, int len) throws IOException {
		byte[] res = new byte[len];
		int n = 0;
		while (n < len) {
		    int count = in.read(res, n, len - n);
		    if (count < 0) {
		        throw new EOFException(); // should not occur
		    }
		    n += count;
		}
		return res;
	}


}
