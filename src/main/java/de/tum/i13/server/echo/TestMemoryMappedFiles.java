package de.tum.i13.server.echo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TestMemoryMappedFiles {
    static int length = 1024;

    public static void main(String[] args) {
        {
            try(RandomAccessFile file = new RandomAccessFile("WAL.dat", "rw"))
            {
                MappedByteBuffer out = file.getChannel()
                        .map(FileChannel.MapMode.READ_WRITE, 0, length);

                for (int i = 0; i < 10; i++)
                {
                    byte[] str = ("Hello"+i+"\n").getBytes();
                    out.put(str);
                }
                System.out.println("Finished writing");



//                byte[] line = new byte[6];
//                out.position(0);
//                out.get(line, 0, line.length);

//                System.out.println(new String(line));

                System.out.println(out.position());

                System.out.println(out.remaining());


            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
