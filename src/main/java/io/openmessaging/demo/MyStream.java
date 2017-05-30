package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;

/**
 * Created by Then on 2017/5/28.
 */
public class MyStream {
    BufferedOutputStream bufferedOutputStream;
    BufferedInputStream bufferedInputStream;
    private  int CACHE_SIZE ;
    private int cacheLen=0;
    private  int readoffset=-1; //ztg
    private boolean fileEnd = false;
    private byte[] cacheBytes ;
    public final static int WRITE=0;
    public final static int READ=1;

    public MyStream(String path,int mode) throws FileNotFoundException {
        if(mode == WRITE){
            CACHE_SIZE = 256 * 1024;
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(new File(path)));
        }else{
            CACHE_SIZE = 16 * 1024 * 1024;
            bufferedInputStream = new BufferedInputStream(new FileInputStream(new File(path)));
        }
        cacheBytes = new byte[CACHE_SIZE];
    }

    public void  write(Message message) throws IOException {
        synchronized(this){
            byte[] serializeBytes=SerializeUtil.serialize((DefaultBytesMessage)message);
            int messageLen = serializeBytes.length;
            serializeBytes=SerializeUtil.byteMerger(SerializeUtil.intToByteArray(messageLen),serializeBytes);
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
   /*     byte[] bytes = new byte[4];
        if(bufferedInputStream.read(bytes)==-1){
            return null;
        }
        int mesagelen=SerializeUtil.byteArrayToInt(bytes);
        //根据message长度读取message信息
        bytes = new byte[mesagelen];
        bufferedInputStream.read(bytes);
        DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
        return defaultBytesMessage;*/          //Created by Then
        //第一次读，进行初始化
        if(readoffset==-1){
            int fileSize=bufferedInputStream.read(cacheBytes);
            readoffset=0;
            if(fileSize <CACHE_SIZE){
                CACHE_SIZE = fileSize;
                fileEnd = true;
            }
            byte[] bytes = new byte[4];
            System.arraycopy(cacheBytes,readoffset,bytes,0,4);
            int mesagelen=SerializeUtil.byteArrayToInt(bytes);
            readoffset=readoffset+4;
            bytes = new byte[mesagelen];
            System.arraycopy(cacheBytes,readoffset,bytes,0,mesagelen);
            readoffset=mesagelen+readoffset;
            return (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
        }
        //缓存剩余大小不足4个字节，需要读文件到缓存中
        else if(readoffset+4>CACHE_SIZE){
            if(fileEnd){
                return null;
            }
            System.arraycopy(cacheBytes,readoffset,cacheBytes,0,CACHE_SIZE-readoffset);
            byte[] bytes = new byte[readoffset];
            int readedSize = bufferedInputStream.read(bytes);
            if(readedSize == -1){
                fileEnd=true;
                return null;
            }
            System.arraycopy(bytes,0,cacheBytes,CACHE_SIZE-readoffset,readedSize);
            if(readedSize < readoffset){
                CACHE_SIZE = readedSize + CACHE_SIZE-readoffset;
                fileEnd = true;
            }
            bytes = new byte[4];
            readoffset=0;
            System.arraycopy(cacheBytes,0,bytes,0,4);
            readoffset+=4;
            int messagelen=SerializeUtil.byteArrayToInt(bytes);
            bytes = new byte[messagelen];
            System.arraycopy(cacheBytes,readoffset,bytes,0,messagelen);
            readoffset+=messagelen;
            return (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
        }
        //缓存剩余大小大于4个字节
        else{
            byte[] bytes = new byte[4];
            System.arraycopy(cacheBytes,readoffset,bytes,0,4);
            int mesagelen=SerializeUtil.byteArrayToInt(bytes);
            readoffset+=4;
            //下一条消息全在缓存中
            if (readoffset+mesagelen<=CACHE_SIZE){
                bytes = new byte[mesagelen];
                System.arraycopy(cacheBytes,readoffset,bytes,0,mesagelen);
                readoffset += mesagelen;
                return (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
            }
            //下一条消息有一部分在文件中，需要读文件
            else {
                System.arraycopy(cacheBytes,readoffset,cacheBytes,0,CACHE_SIZE-readoffset);
                bytes = new byte[readoffset];
                int readedSize = bufferedInputStream.read(bytes);
                System.arraycopy(bytes,0,cacheBytes,CACHE_SIZE-readoffset,readedSize);
                if(readedSize < readoffset){
                    CACHE_SIZE = readedSize + CACHE_SIZE-readoffset;
                    fileEnd = true;
                }
                readoffset=0;
                bytes = new byte[mesagelen];
                System.arraycopy(cacheBytes,readoffset,bytes,0,mesagelen);
                readoffset=mesagelen+readoffset;
                return (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
            }
        }
    }
    public  void writeCache() throws IOException {
        bufferedOutputStream.write(cacheBytes,0,cacheLen);
    }
    public void close() throws IOException {
       bufferedOutputStream.close();
    }
}
