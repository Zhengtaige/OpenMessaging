package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

public class ReadStore {
    private static final ReadStore INSTANCE = new ReadStore();

    private HashMap<String, FileChannel> map = new HashMap<>(); //key:队列或Topic名字，value：对应文件的Channel

    private ReadStore() {
        try {
            File file = new File("./test");
            for (String path : file.list()) {
                map.put(path, new RandomAccessFile("./test/" + path, "rw").getChannel());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ReadStore getInstance() {
        return INSTANCE;
    }

    public String getMessage(String name) {
        return read(map.get(name));
    }


    private String read(FileChannel channel) {
        ByteBuffer buffer = ByteBuffer.allocate(256 * 1024);
        int bytesread = 0;
        try {
            bytesread = channel.read(buffer);
            if (bytesread == -1) {
                channel.close();
                return "over";
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("读取文件出错");
        }

        byte[] bytes = new byte[256 * 1024];
        buffer.rewind();
        buffer.get(bytes);
        return new String("ok" + Thread.currentThread());
    }
}
