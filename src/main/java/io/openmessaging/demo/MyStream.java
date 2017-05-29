package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;

/**
 * Created by Then on 2017/5/28.
 */
public class MyStream {
    BufferedOutputStream bufferedOutputStream;
    BufferedInputStream bufferedInputStream;
    private final int CACHE_SIZE = 256 * 1024;
    private int cacheLen=0;
    private byte[] cacheBytes = new byte[CACHE_SIZE];
    public final static int WRITE=0;
    public final static int READ=1;

    public MyStream(String path,int mode) throws FileNotFoundException {
        if(mode == WRITE){
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(new File(path)));
        }else{
            bufferedInputStream = new BufferedInputStream(new FileInputStream(new File(path)));
        }
    }

    public void  write(Message message) throws IOException {
        synchronized(this){
            byte[] serializeBytes=SerializeUtil.serialize((DefaultBytesMessage)message);

            if(serializeBytes.length+cacheLen>CACHE_SIZE){
                bufferedOutputStream.write(cacheBytes,0,cacheLen);
                System.arraycopy(serializeBytes,0,cacheBytes,0,serializeBytes.length);
                cacheLen = serializeBytes.length;
            }else {
                System.arraycopy(serializeBytes,0,cacheBytes,cacheLen,serializeBytes.length);
                cacheLen += serializeBytes.length;
            }
        }
    }

    public Message read() throws IOException {
        //读取message长度数据，4个字节
        byte[] bytes = new byte[4];
        if(bufferedInputStream.read(bytes)==-1){
            return null;
        }
        int mesagelen=SerializeUtil.byteArrayToInt(bytes);
        //根据message长度读取message信息
        bytes = new byte[mesagelen];
        bufferedInputStream.read(bytes);
        DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
        return defaultBytesMessage;
    }
    public  void writeCache() throws IOException {
        bufferedOutputStream.write(cacheBytes,0,cacheLen);
    }
    public void close() throws IOException {
       bufferedOutputStream.close();
    }
}
