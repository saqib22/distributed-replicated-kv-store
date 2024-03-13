package de.tum.i13.server.echo;

import de.tum.i13.shared.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class WAL {
    /*
    * Wrtie-ahead-logging using Memory-Mapped I/O
    * @File Format:
    *   @Fields: Command (String), Sync(boolean - 0,1)
    * */
    private RandomAccessFile file;
    private static MappedByteBuffer buffer;
    private int next_entry_position;
    private int newVals;

    public WAL() throws IOException {
        this.next_entry_position = 0;
        this.file =  new RandomAccessFile("WAL.dat", "rw");
        buffer = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Constants.WAL_SIZE);
        newVals = 0;
    }

    public int getNewVals(){
        return newVals;
    }
    public void setNewVals(int newVal){
        newVals = newVal;
    }
    synchronized String wal_process(String op, String... args){
        switch (op){
            case "add":{
                buffer.position(this.next_entry_position);
                byte[] str = (args[1]+";"+0+"\n").getBytes();
                buffer.put(str);
                this.next_entry_position = buffer.position();
                newVals += 1;
                return null;
            }

            case "update_sync":{
                int sync_pos = Integer.parseInt(args[0]);
                buffer.position(sync_pos);
                for (int i = 0; (char) buffer.get() != ';'; i++) {
                    sync_pos += 1;
                }
                buffer.put((byte) '1');
                return String.valueOf(sync_pos +=3);
            }

            case "get":{
                buffer.position(Integer.parseInt(args[0]));
                StringBuilder entry = new StringBuilder();
                int i = 0;
                char temp = 0;
                for (; temp != '\n'; i++){
                    temp = (char) buffer.get();
                    entry.append(temp);
                }
                return entry.toString().split(";")[0];
            }
        }
        return null;
    }
    public boolean remaining(){
        buffer.position(this.next_entry_position);
        return buffer.remaining() + buffer.position() != buffer.capacity();
    }
}
