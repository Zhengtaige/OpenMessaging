package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Then on 2017/5/27.
 */
public class MyFileChannel {

    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private final int MAP_SIZE = 256 * 1024 *1024;
    public final static int WRITE=0;
    public final static int READ=1;
   public MyFileChannel(String path,int mode){
       super();
       if(mode == WRITE){
           try {
               fileChannel = new RandomAccessFile(path,"rw").getChannel();
               mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,MAP_SIZE);
           } catch (FileNotFoundException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }
       }else{
           try {
               fileChannel = new RandomAccessFile(path,"rw").getChannel();
               mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,0,MAP_SIZE);
           } catch (FileNotFoundException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }
       }

   }

   public int write(Message message) throws IOException {
       int ret = -1;
       byte[] serializeBytes=((DefaultBytesMessage)message).getBytess();
       ByteBuffer byteBuffer;
       synchronized (mappedByteBuffer) {
           byteBuffer  = mappedByteBuffer.slice();
           mappedByteBuffer.position(mappedByteBuffer.position()+serializeBytes.length);
       }
       byteBuffer.put(serializeBytes);
       return ret;
   }

   public Message read(int i, int []offsetArray) throws IOException {
       //读取message长度数据，4个字节
       byte[] bytes;
       int len;
       ByteBuffer byteBuffer;
       synchronized (mappedByteBuffer) {
           mappedByteBuffer.position(offsetArray[i]);
           byteBuffer = mappedByteBuffer.slice();
       }
       len = byteBuffer.getInt();
       bytes = new byte[len];
       byteBuffer.get(bytes);
       Message message =  SerializeUtil.unserialize(bytes);
       offsetArray[i] += len + 4;
       return message;
   }
}
