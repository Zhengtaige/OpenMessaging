package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private static String path;
    private static Map<String, MyStream> streamMap = new HashMap<>();
    private boolean closing = false;
    private AtomicInteger atomicInteger = new AtomicInteger();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    public static void setPath(String path) {
        synchronized (MessageStore.class){
            if (MessageStore.path != null) return;
        }
        MessageStore.path = path;
        try {
            Files.createDirectories(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public  synchronized void putMessage(String bucket, Message message) throws IOException {
        MyStream myStream=null;
        if(!streamMap.containsKey(bucket)){
            myStream= new MyStream(path+"\\"+bucket+".ms",MyStream.WRITE);
            streamMap.put(bucket,myStream);
        }else{
            myStream = streamMap.get(bucket);
        }
        myStream.write(message);
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
       MyStream myStream=null;
       if(!streamMap.containsKey(bucket)){
           myStream= new MyStream(path+"\\"+bucket+".ms", MyStream.READ);
           streamMap.put(bucket,myStream);
       }else{
           myStream = streamMap.get(bucket);
       }
       return myStream.read();
   }



    public void closeStream() throws IOException {
        synchronized (this) {
            if (closing) return;
            closing = true;
        }
        Iterator<Map.Entry<String, MyStream>> iterator = streamMap.entrySet().iterator();
        while(iterator.hasNext()){
            MyStream mystream = iterator.next().getValue();
            mystream.writeCache();
            mystream.close();
        }
        streamMap.clear();
    }
}

