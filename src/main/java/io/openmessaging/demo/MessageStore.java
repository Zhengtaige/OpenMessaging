package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private Map<String,FileChannel> fileChannelMap = new HashMap<>();


    public synchronized void putMessage(String bucket, Message message) throws IOException {
        if(!fileChannelMap.containsKey(bucket)){
            FileOutputStream fi=null;
            try {
                fi = new FileOutputStream(new File(bucket+".ms"));

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            FileChannel fileChannel = fi.getChannel();
            fileChannelMap.put(bucket,fileChannel);
        }
        FileChannel fileChannel = fileChannelMap.get(bucket);
        byte[] serializeBytes=SerializeUtil.serialize((DefaultBytesMessage)message);
        int messagelength=serializeBytes.length;
        byte[] infosizetag=intToByteArray(messagelength);
        ByteBuffer buf = ByteBuffer.allocate(messagelength+4);
        buf.put(byteMerger(infosizetag,serializeBytes));
        buf.rewind();
        fileChannel.write(buf);
    }

   public synchronized Message pullMessage(String queue, String bucket) throws IOException {
//        ArrayList<Message> bucketList = messageBuckets.get(bucket);
//        if (bucketList == null) {
//            return null;
//        }
//        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
//        if (offsetMap == null) {
//            offsetMap = new HashMap<>();
//            queueOffsets.put(queue, offsetMap);
//        }
//        int offset = offsetMap.getOrDefault(bucket, 0);
//        if (offset >= bucketList.size()) {
//            return null;
//        }
//        Message message = bucketList.get(offset);
//        offsetMap.put(bucket, ++offset);
       if(!fileChannelMap.containsKey(bucket)){
           FileOutputStream fi=null;
           try {
               fi = new FileOutputStream(new File(bucket+".ms"));

           } catch (FileNotFoundException e) {
               e.printStackTrace();
           }
           FileChannel fileChannel = fi.getChannel();
           fileChannelMap.put(bucket,fileChannel);
       }
       FileChannel fileChannel = fileChannelMap.get(bucket);
       //读取message长度数据，4个字节
       ByteBuffer byteBuffer = ByteBuffer.allocate(4);
       if(fileChannel.read(byteBuffer)==-1){
           return null;
       }
       byteBuffer.rewind();
       byte[] bytes = new byte[4];
       byteBuffer.get(bytes);
       int mesagelen=byteArrayToInt(bytes);
       //根据message长度读取message信息
       byteBuffer = ByteBuffer.allocate(mesagelen);
       fileChannel.read(byteBuffer);
       bytes = new byte[mesagelen];
       byteBuffer.rewind();
       byteBuffer.get(bytes);
       DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) SerializeUtil.unserialize(bytes);
       return defaultBytesMessage;
   }

    public byte[] byteMerger(byte[] byte_1, byte[] byte_2){
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

