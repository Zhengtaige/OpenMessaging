package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private String path;
    private static Map<String, MyStream> streamMap = new HashMap<>();
    public ConcurrentHashMap<String, ConcurrentLinkedQueue<DefaultPullConsumer>> bucketConsumerMap = new ConcurrentHashMap<>() ;
    public static MessageStore getInstance() {
        return INSTANCE;
    }

    public AtomicInteger num = new AtomicInteger();

    public void setPath(String path) {
        synchronized (MessageStore.class){
            if (this.path != null) return;
        }
        this.path = path;
        try {
            Files.createDirectories(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public   void putMessage(String bucket, Message message) throws IOException {
        MyStream myStream;
        synchronized (this){
            if(!streamMap.containsKey(bucket)){
                myStream= new MyStream(path+"\\"+bucket+".ms",MyStream.WRITE);
                streamMap.put(bucket,myStream);
            }else{
                myStream = streamMap.get(bucket);
            }
        }
        myStream.write(message);
        num.getAndIncrement();
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
       MyStream myStream;
       if(!streamMap.containsKey(bucket)){
           myStream= new MyStream(path+"\\"+bucket+".ms", MyStream.READ);
           streamMap.put(bucket,myStream);
       }else{
           myStream = streamMap.get(bucket);
       }
       synchronized (myStream) {
           ConcurrentLinkedQueue<DefaultPullConsumer> cons = bucketConsumerMap.get(bucket);
           Message message = myStream.read();
           if(message == null){
//               bucketConsumerMap.remove(bucket);
               return null;
           }
           for (DefaultPullConsumer consumer :
                   cons ) {
               consumer.messageQueue.add(message);
           }
           return message;
       }
   }



    public void closeStream() throws IOException {
        Iterator<Map.Entry<String, MyStream>> iterator = streamMap.entrySet().iterator();
        while(iterator.hasNext()){
            MyStream mystream = iterator.next().getValue();
            mystream.writeCache();
            mystream.close();
        }
        streamMap.clear();
        System.out.println("writed:"+num.get());
    }
}

