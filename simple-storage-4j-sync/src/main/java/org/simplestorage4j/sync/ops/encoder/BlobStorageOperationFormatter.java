package org.simplestorage4j.sync.ops.encoder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.BlobStorageRepository;
import org.simplestorage4j.sync.ops.BlobStorageOperation;
import org.simplestorage4j.sync.ops.CopyFileStorageOperation;
import org.simplestorage4j.sync.ops.MkdirStorageOperation;
import org.simplestorage4j.sync.ops.ZipCopyFileStorageOperation;
import org.simplestorage4j.sync.ops.ZipCopyFileStorageOperation.SrcStorageZipEntry;

import com.google.common.collect.ImmutableList;

import lombok.RequiredArgsConstructor;
import lombok.val;

public class BlobStorageOperationFormatter {

	// ------------------------------------------------------------------------
	
	/**
	 * 
	 */
	public static class BlobStorageOperationWriter {
		
		private final PrintWriter out;

		public BlobStorageOperationWriter(PrintWriter out) {
			this.out = out;
		}
		
		public void write(BlobStorageOperation op) {
			out.print(op.taskId);
			printSep();
			if (op instanceof MkdirStorageOperation) {
				val op2 = (MkdirStorageOperation) op;
				// format: "${taskId}:d:${destStorageId}:${destPath}"
				print('d');
				printSep();
				printStorageAndPath(op2.storagePath);
				printLine();
			
			} else if (op instanceof ZipCopyFileStorageOperation) {
				val op2 = (ZipCopyFileStorageOperation) op;
				// format: "${taskId}:z:${destStorageId}:${destPath}:${srcStorageId}:${entryCount}"
				// then sub-lines:  "e:${destEntryPath}:${srcStoragePath}"
				print('z');
				printSep();
				printStorageAndPath(op2.destStoragePath);
				printSep();
				print(op2.srcStorage.id.id);
				printSep();
				print(op2.srcEntries.size());
				printLine();
				for(val srcEntry: op2.srcEntries) {
					print('e');
					printSep();
					printQuoted(srcEntry.destEntryPath);
					printSep();
					printQuoted(srcEntry.srcStoragePath);
					printSep();
					print(srcEntry.srcFileLen);
					printLine();
				}
			
			} else if (op instanceof CopyFileStorageOperation) {
				val op2 = (CopyFileStorageOperation) op;
				// format: "${taskId}:f:${destStorageId}:${destPath}:${srcStorageId}:${srcPath}:${fisrcFileLen}"
				print('f');
				printSep();
				printStorageAndPath(op2.destStoragePath);
				printSep();
				printStorageAndPath(op2.srcStoragePath);
				printSep();
				print(op2.srcFileLen);
				printLine();
				
			} else {
				throw new IllegalStateException("should not occur");
			}
		}
		
		protected void print(String value) {
			// assume no separator ':' in value
			out.append(value);
		}
		protected void print(char value) {
			out.append(value);
		}
		protected void print(long value) {
			out.print(value);
		}
		protected void printLine() {
			out.append('\n');
		}
		protected void printSep() {
			out.append(':');
		}
		
		protected void printStorageAndPath(BlobStoragePath src) {
			out.append(src.blobStorage.id.id);
			out.append(":");
			printQuoted(src.path);
		}
		
		protected void printQuoted(String path) {
			out.append("\"");
			out.append(path); // TODO escape '"'
			out.append("\"");
	    }

	}
    
	// ------------------------------------------------------------------------

	/**
	 * 
	 */
	public static class BlobStorageOperationReader {
		
		private final BufferedReader lineReader;
		private final BlobStorageRepository blobStorages;

		public BlobStorageOperationReader(BufferedReader lineReader, BlobStorageRepository blobStorages) {
			this.lineReader = lineReader;
			this.blobStorages = blobStorages;
		}

		public BlobStorageOperation read() throws IOException {
			val line = lineReader.readLine();
			if (line == null) {
				return null;
			}
			val fieldReader = new SeparatedFieldReader(line);
			val taskId = fieldReader.readInt();
	        val taskType = fieldReader.readChar(); // expect 'd', 'z', 'f'
	        
	        BlobStorageOperation res;
	        switch(taskType) {
	        case 'd': {
	        	val blobStoragePath = readBlobStoragePath(fieldReader);
	            res = new MkdirStorageOperation(taskId, blobStoragePath);
	        } break;

	        case 'z': {
	        	// format: "${taskId}:z:${destStorageId}:${destPath}:${srcStorageId}:${entryCount}"
				// then sub-lines:  "e:${destEntryPath}:${srcStoragePath}"
	        	val destStoragePath = readBlobStoragePath(fieldReader);
	        	val srcStorage = readBlobStorage(fieldReader);
	        	int entryCount = fieldReader.readInt();
	        	val srcEntries = ImmutableList.<SrcStorageZipEntry>builder(); 
	        	for(int i = 0; i < entryCount; i++) {
	        		String entryLine = lineReader.readLine();
	        		if (entryLine == null) {
	        			throw new IllegalStateException("expected " + entryCount + " entries, got " + i);
	        		}
	        		val entryFieldReader = new SeparatedFieldReader(entryLine);
	        		entryFieldReader.readExpectedChar('e');
	        		entryFieldReader.readSep();
	        		val destEntryPath = entryFieldReader.readQuoted();
	        		entryFieldReader.readSep();
	        		val srcPath = entryFieldReader.readQuoted();
	        		entryFieldReader.readSep();
	        		val srcFileLen = entryFieldReader.readLong();
	        		srcEntries.add(new SrcStorageZipEntry(destEntryPath, srcPath, srcFileLen));
	        	}
	            res = new ZipCopyFileStorageOperation(taskId, //
	            		destStoragePath, srcStorage, srcEntries.build());
	        } break;
	        
	        case 'f': {
	        	val destStoragePath = readBlobStoragePath(fieldReader);
	        	val srcStoragePath = readBlobStoragePath(fieldReader);
	        	val srcFileLen = fieldReader.readLong();
	            res = new CopyFileStorageOperation(taskId, destStoragePath, srcStoragePath, srcFileLen);
	        } break;
	        
	        default:
	            throw new IllegalStateException("should not occur expected taskType {d|z|f}, got " + line);
	        }
	        return res;
	    }

		protected BlobStoragePath readBlobStoragePath(
				SeparatedFieldReader fieldReader) {
			val blobStorage = readBlobStorage(fieldReader);
			val path = fieldReader.readQuoted();
			val blobStoragePath = new BlobStoragePath(blobStorage, path);
			return blobStoragePath;
		}

		private org.simplestorage4j.api.BlobStorage readBlobStorage(SeparatedFieldReader fieldReader) {
			val blobStorageId = fieldReader.readString();
			val blobStorage = blobStorages.get(new BlobStorageId(blobStorageId));
			return blobStorage;
		}
	
	    protected static int nextUnescapedQuote(String text, int startPos) {
	        int res = text.indexOf('"', startPos); 
	        // TOADD while previous char is escaping '\' .. search next
	        return res;
	    }
	    
	    protected String unquotePath(String quotedPath) {
	        if (quotedPath.charAt(0) != '"' || quotedPath.charAt(quotedPath.length()-1) != '"') {
	            throw new IllegalArgumentException();
	        }
	        val path = quotedPath.substring(1, quotedPath.length()-1);
	        return path;
	    }

	}

	@RequiredArgsConstructor
	protected static class SeparatedFieldReader {
		private final String line;
		private int currPos;

		public void readExpectedChar(char ch) {
			val checkCh = line.charAt(currPos);
			if (checkCh != ch) {
				throw new IllegalStateException("expected '" + ch + "' at " + currPos +", got '" + checkCh + "' " + line);
			}
			this.currPos++;
		}

		public void readSep() {
			readExpectedChar(':');
		}

		public char readChar() {
			val res = line.charAt(currPos);
			this.currPos++;
			return res;
		}
		
		public int readInt() {
			return (int) readLong();
		}
		public long readLong() {
	        int nextSepIdx = nextSepIdx();
	        val field = line.substring(currPos, nextSepIdx);
	        val res = Long.parseLong(field);
	        this.currPos = nextSepIdx+1;
	        return res;
		}

		public String readString() {
	        int nextSepIdx = nextSepIdx();
	        val res = line.substring(currPos, nextSepIdx);
	        this.currPos = nextSepIdx+1;
	        return res;
		}

		public String readQuoted() {
	        int nextSepIdx = line.indexOf('"', currPos+1); // TODO check for escape..
	        val res = line.substring(currPos, nextSepIdx);
	        this.currPos = nextSepIdx+1;
	        return res;
		}

		private int nextSepIdx() {
			int nextSepIdx = line.indexOf(':', currPos+1);
	        if (nextSepIdx == -1) {
	        	nextSepIdx = line.length();
	        }
			return nextSepIdx;
		}

	}
	
}