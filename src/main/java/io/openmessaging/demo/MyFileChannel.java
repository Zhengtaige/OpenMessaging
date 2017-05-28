package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Then on 2017/5/27.
 */
public class MyFileChannel {

    public final static int WRITE = 0;
    public final static int READ = 1;
    private final int CACHE_SIZE = 256 * 1024;
    //    BufferedInputStream bufferedInputStream;
//    BufferedOutputStream bufferedOutputStream;
    private FileChannel fileChannel;
    private AtomicInteger atomicInteger = new AtomicInteger();
    private int cacheLen = 0;
    //    private byte[] cacheBytes = new byte[CACHE_SIZE];
    private ByteBuffer buf = ByteBuffer.allocate(CACHE_SIZE);


    public MyFileChannel(String path, int mode) {
        super();
        if (mode == WRITE) {
            FileOutputStream fi = null;
            try {
                fi = new FileOutputStream(new File(path + ".ms"));

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            fileChannel = fi.getChannel();
//           bufferedOutputStream = new BufferedOutputStream(fi);
        } else {
            FileInputStream fi = null;
            try {
                fi = new FileInputStream(new File(path + ".ms"));

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            fileChannel = fi.getChannel();
//           bufferedInputStream = new BufferedInputStream(fi);
        }

    }

    public int write(Message message) throws IOException {
        int ret = -1;
        byte[] serializeBytes = SerializeUtil.serialize((DefaultBytesMessage) message);
        int messagelength = serializeBytes.length;
        byte[] infosizetag = SerializeUtil.intToByteArray(messagelength);
        byte[] tmpbytes = SerializeUtil.byteMerger(infosizetag, serializeBytes);
//       System.out.println(cacheLen);
        if (tmpbytes.length + cacheLen > CACHE_SIZE) {
//           buf.put(cacheBytes,0,cacheLen);
            buf.rewind();
            byte[] t = new byte[cacheLen];
            buf.get(t, 0, cacheLen);
            ByteBuffer temp = ByteBuffer.allocate(cacheLen).put(t);
            while (temp.hasRemaining()) {
                ret = fileChannel.write(temp);
            }

            if (ret != 337) System.out.println("write" + ret);

//           bufferedOutputStream.write(cacheBytes,0,cacheLen);
//           System.arraycopy(tmpbytes,0,cacheBytes,0,tmpbytes.length);
            buf.clear();
            buf.put(tmpbytes, 0, tmpbytes.length);
            cacheLen = tmpbytes.length;

        } else if (cacheLen > 0) {
//           System.arraycopy(tmpbytes,0,cacheBytes,cacheLen,tmpbytes.length);
            buf.put(tmpbytes, 0, tmpbytes.length);
            cacheLen += tmpbytes.length;
            ret = 0;
        } else if (cacheLen == 0) {
//           System.arraycopy(tmpbytes,0,cacheBytes,0,tmpbytes.length);
            buf.put(tmpbytes, 0, tmpbytes.length);
            cacheLen = tmpbytes.length;
            ret = 0;
        }
        return ret;
    }

    public Message read() throws IOException {
        //读取message长度数据，4个字节
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        if (fileChannel.read(byteBuffer) == -1) {
            return null;
        }
        byteBuffer.rewind();
        byte[] bytes = new byte[4];
        byteBuffer.get(bytes);
        int mesagelen = SerializeUtil.byteArrayToInt(bytes);
        if (mesagelen > 336) {
            System.out.println(mesagelen);
        }
        //根据message长度读取message信息
        byteBuffer = ByteBuffer.allocate(mesagelen);
        fileChannel.read(byteBuffer);
        bytes = new byte[mesagelen];
        byteBuffer.rewind();
        byteBuffer.get(bytes);
        DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
        return defaultBytesMessage;
    }

    public void close() throws IOException {
        fileChannel.close();
//       bufferedOutputStream.close();
    }

    public void force() throws IOException {
//       ByteBuffer buf = ByteBuffer.allocate(cacheLen);
//       buf.put(cacheBytes,0,cacheLen);
        buf.rewind();
        fileChannel.write(buf);
        fileChannel.force(false);
    }
}
