package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private static String path;
    private static Map<String, MyFileChannel> fileChannelMap = new HashMap<>();
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
        MyFileChannel myfileChannel=null;
        if(!fileChannelMap.containsKey(bucket)){
            myfileChannel= new MyFileChannel(path+"\\"+bucket,MyFileChannel.WRITE);
            fileChannelMap.put(bucket,myfileChannel);
        }else{
            myfileChannel = fileChannelMap.get(bucket);
        }
        myfileChannel.write(message);
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
       MyFileChannel myfileChannel=null;
       if(!fileChannelMap.containsKey(bucket)){
           myfileChannel= new MyFileChannel(path+"\\"+bucket, MyFileChannel.READ);
           fileChannelMap.put(bucket,myfileChannel);
       }else{
           myfileChannel = fileChannelMap.get(bucket);
       }
       return myfileChannel.read();
   }



    public void closeFilechannel() throws IOException {
        synchronized (this) {
            if (closing) return;
            closing = true;
        }
        Iterator<Map.Entry<String, MyFileChannel>> iterator = fileChannelMap.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String, MyFileChannel> entry = iterator.next();
            entry.getValue().force();
            entry.getValue().close();
        }
        fileChannelMap.clear();
    }

    public void force() throws IOException {
        Iterator<Map.Entry<String, MyFileChannel>> iterator = fileChannelMap.entrySet().iterator();
        while(iterator.hasNext()){
            iterator.next().getValue().force();
        }
    }
}

