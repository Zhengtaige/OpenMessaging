package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private String path;
    private static Map<String, MyStream> streamMap = new HashMap<>();
    private static ConcurrentLinkedQueue<Message> messageQue = new ConcurrentLinkedQueue<>();

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

    public void putInQue(Message m) {
        messageQue.add(m);
        Message message = messageQue.poll();
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        //一个message要么是topic里面的要么是queue里面的，这个是在初始化数据的时候定的，BytesMessage createBytesMessageToTopic(String topic, byte[] body)，topic作为了header
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        try {
            putMessage(topic != null ? topic : queue, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public  void putMessage(String bucket, Message message) throws IOException {
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
       return myStream.read();
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

