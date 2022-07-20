package org.simplestorage4j.api;

public class BlobStoreFileInfo {

    public final String path;
    public final boolean isDir;
    public final long fileLength;
    public final long lastModifTime;


    public BlobStoreFileInfo(String path, boolean isDir, long fileLength, long lastModifTime) {
        if (path.endsWith("/")) {
            path = path.substring(0, path.length()-1);
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        this.path = path;
        this.isDir = isDir;
        this.fileLength = fileLength;
        this.lastModifTime = lastModifTime;
    }

    public String childName() {
        int lastSep = path.lastIndexOf('/') ;
        if (lastSep == -1) {
            return path;
        }
        if (lastSep == path.length()-1) {
            // should not occur... endind with '/' ??
            return "";
        }
        return path.substring(lastSep + 1);
    }

}