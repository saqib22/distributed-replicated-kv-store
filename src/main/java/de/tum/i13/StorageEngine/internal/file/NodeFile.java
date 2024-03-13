package de.tum.i13.StorageEngine.internal.file;

public interface NodeFile {
    long position();
    
    void position(long position);
}
