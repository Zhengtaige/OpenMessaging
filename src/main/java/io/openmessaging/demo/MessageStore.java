package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
            FileInputStream fi=null;
            try {
                fi = new FileInputStream(new File(bucket+".ms"));

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            FileChannel fileChannel = fi.getChannel();
            fileChannelMap.put(bucket,fileChannel);
        }
        FileChannel fileChannel = fileChannelMap.get(bucket);
        String messageInfo=message.toString();
        int messagelength=messageInfo.length();
        char[] infosizetag=new char[3];
        infosizetag[0]=(char) (messagelength&15);
        infosizetag[1]=(char) ((messagelength>>8)&15);
        infosizetag[2]=(char) ((messagelength>>8)&15);
        ByteBuffer buf = ByteBuffer.allocate(messagelength);
        fileChannel.write(buf);
        fileChannel.close();
    }

   public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
   }
}

