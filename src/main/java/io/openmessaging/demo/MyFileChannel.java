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

    private FileChannel fileChannel;
    private final int CACHE_SIZE = 256 * 1024;
    private int cacheLen=0;
    private byte[] cacheBytes = new byte[CACHE_SIZE];
    public final static int WRITE=0;
    public final static int READ=1;


   public MyFileChannel(String path,int mode){
       super();
       if(mode == WRITE){
           FileOutputStream fi=null;
           try {
               fi = new FileOutputStream(new File(path+".ms"));

           } catch (FileNotFoundException e) {
               e.printStackTrace();
           }
           fileChannel = fi.getChannel();
       }else{
           FileInputStream fi=null;
           try {
               fi = new FileInputStream(new File(path+".ms"));

           } catch (FileNotFoundException e) {
               e.printStackTrace();
           }
           fileChannel = fi.getChannel();
       }

   }

   public int write(Message message) throws IOException {

           int ret = -1;
           byte[] serializeBytes=SerializeUtil.serialize((DefaultBytesMessage)message);
           int messagelength=serializeBytes.length;
           byte[] infosizetag=SerializeUtil.intToByteArray(messagelength);
           byte[] tmpbytes=SerializeUtil.byteMerger(infosizetag,serializeBytes);
       synchronized (this){
           if(tmpbytes.length+cacheLen>CACHE_SIZE){
               ByteBuffer buf = ByteBuffer.allocate(cacheLen);
               buf.put(cacheBytes,0,cacheLen);
               buf.rewind();
               ret = fileChannel.write(buf);
               System.arraycopy(tmpbytes,0,cacheBytes,0,tmpbytes.length);
               cacheLen = tmpbytes.length;
           }else {
               System.arraycopy(tmpbytes,0,cacheBytes,cacheLen,tmpbytes.length);
               cacheLen += tmpbytes.length;
               ret = 0;
           }
       }
       return ret;
   }

   public Message read() throws IOException {
       //读取message长度数据，4个字节
       ByteBuffer byteBuffer = ByteBuffer.allocate(4);
       if(fileChannel.read(byteBuffer)==-1){
           return null;
       }
       byteBuffer.rewind();
       byte[] bytes = new byte[4];
       byteBuffer.get(bytes);
       int mesagelen=SerializeUtil.byteArrayToInt(bytes);
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
   }

   public void force() throws IOException {
       ByteBuffer buf = ByteBuffer.allocate(cacheLen);
       buf.put(cacheBytes,0,cacheLen);
       buf.rewind();
       fileChannel.write(buf);
       fileChannel.force(false);
   }
}
