package io.openmessaging.demo;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by autulin on 2017/5/12.
 */
public class TestMain {
    public static void main(String[] args) throws IOException {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage("wxj".getBytes());
        defaultBytesMessage.putHeaders("header1","h111");
        defaultBytesMessage.putProperties("properties","p111");

        //存文件
        FileOutputStream fileOutputStream = new FileOutputStream(new File("test.txt"));
        FileChannel fileChannel=fileOutputStream.getChannel();
        byte[] serializebytes=SerializeUtil.serialize(defaultBytesMessage);
        ByteBuffer byteBuffer = ByteBuffer.allocate(serializebytes.length+4);
        int messagelength=serializebytes.length;
        byte[] infosizetag=intToByteArray(messagelength);
        byteBuffer.put(byteMerger(infosizetag,serializebytes));
        byteBuffer.rewind();
        fileChannel.write(byteBuffer);
        fileChannel.close();

        //读文件
        FileInputStream fileInputStream = new FileInputStream(new File("test.txt"));
        FileChannel fileChannel2 = fileInputStream.getChannel();
        //读message的长度标识（4个字节
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(4);
        fileChannel2.read(byteBuffer2);
        byte[] lenbyte=new byte[4];
        byteBuffer2.rewind();
        byteBuffer2.get(lenbyte);
        int messagelen=byteArrayToInt(lenbyte);
        //读message实体
        byteBuffer2=ByteBuffer.allocate(messagelen);
        while(byteBuffer2.hasRemaining()){
            fileChannel2.read(byteBuffer2);
        }
        byte[] serializebytes2=new byte[messagelen];
        byteBuffer2.rewind();
        byteBuffer2.get(serializebytes2);
        DefaultBytesMessage defaultBytesMessage2 =(DefaultBytesMessage)SerializeUtil.unserialize(serializebytes2);
        fileChannel2.close();
    }

    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2){
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }

    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        //由高位到低位
        result[0] = (byte)((i >> 24) & 0xFF);
        result[1] = (byte)((i >> 16) & 0xFF);
        result[2] = (byte)((i >> 8) & 0xFF);
        result[3] = (byte)(i & 0xFF);
        return result;
    }

    /**
     * byte[]转int
     * @param bytes
     * @return
     */
    public static int byteArrayToInt(byte[] bytes) {
        int value= 0;
        //由高位到低位
        for (int i = 0; i < 4; i++) {
            int shift= (4 - 1 - i) * 8;
            value +=(bytes[i] & 0x000000FF) << shift;//往高位游
        }
        return value;
    }
}
