package org.simplestorage4j.sync.ops;
import lombok.val;

public class BlobStorageOperationFormatter {

    public String formatToLine(BlobStorageOperation task) {
        String res;
        if (task instanceof MkdirStorageFileOperation) {
            res = task.taskId + ":d:" + quotePath(task.path);
        } else if (task instanceof ZipCopyStorageFileOperation) {
            val packageTask = (ZipCopyStorageFileOperation) task;
            res = task.taskId + ":z:" + quotePath(task.path) + ":"
                    + packageTask.packageFile.getZipEntries().size() + ":"
                    + LsUtils.mapStrJoin(packageTask.packageFile.getZipEntries().values(), 
                            entry -> quotePath(entry.zipEntryPath) + "," + quotePath(entry.srcPath) + "," + entry.fileLength, ";");
        } else if (task instanceof CopyStorageFileOperation) {
            val copyFileTask = (CopyStorageFileOperation) task;
            res = task.taskId + ":f:" + quotePath(task.path) + ":" + quotePath(copyFileTask.srcPath) + ":" + copyFileTask.srcFileLen;
        } else {
            throw new IllegalStateException("should not occur");
        }
        return res; 
    }
    
    public BlobStorageOperation parseFromLine(String line) {
        int sepIdx = line.indexOf(':');
        val taskIdText = line.substring(0, sepIdx);
        val taskId = Integer.parseInt(taskIdText);
        val taskType = line.charAt(sepIdx + 1); // expect 'd', 'z', 'f'
        val openQuoteIdx = sepIdx + 3;
        if (line.charAt(openQuoteIdx) != '"') {
            throw new IllegalArgumentException("expected quote at " + openQuoteIdx + " " + line);
        }
        val closeQuoteIdx = nextUnescapedQuote(line, openQuoteIdx + 1);
        String quotePath = line.substring(openQuoteIdx, closeQuoteIdx);
        val path = unquotePath(quotePath);
        
        BlobStorageOperation res;
        switch(taskType) {
        case 'd': {
            res = new MkdirStorageFileOperation(taskId, path);
        } break;
        case 'z': {
            res = null; // TODO NOT IMPL YET
            
        } break;
        case 'f': {
            
            res = null; // TODO NOT IMPL YET
        } break;
        default:
            throw new IllegalStateException("should not occur");
        }
        return res;
    }

    protected static int nextUnescapedQuote(String text, int startPos) {
        int res = text.indexOf('"', startPos); 
        // TOADD while previous char is escaping '\' .. search next
        return res;
    }
    
    protected static String quotePath(String path) {
        return "\"" + path + "\"";
    }

    protected String unquotePath(String quotedPath) {
        if (quotedPath.charAt(0) != '"' || quotedPath.charAt(quotedPath.length()-1) != '"') {
            throw new IllegalArgumentException();
        }
        val path = quotedPath.substring(1, quotedPath.length()-1);
        return path;
    }

}
