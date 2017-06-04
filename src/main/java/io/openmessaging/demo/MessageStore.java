package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private static String path;
    private static Map<String, MyFileChannel> fileChannelMap = new HashMap<>();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    public static void setPath(String path) {
        synchronized (MessageStore.class){
            if (MessageStore.path != null) return;
        }
        try {
            for (String filename:
                new File(path).list()) {
                fileChannelMap.put(filename,new MyFileChannel(path+"/"+filename , MyFileChannel.WRITE ));
            }
            MessageStore.path = path;
            Files.createDirectories(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public   void putMessage(String bucket, Message message) throws IOException {
        MyFileChannel myfileChannel;
        myfileChannel = fileChannelMap.get(bucket);
        if(myfileChannel == null){
            synchronized (this){
                if (!fileChannelMap.containsKey(bucket)) {
                    myfileChannel = new MyFileChannel(path + "/" + bucket, MyFileChannel.WRITE);
                    fileChannelMap.put(bucket, myfileChannel);
                }else{
                    myfileChannel = fileChannelMap.get(bucket);
                }
            }
        }
        myfileChannel.write(message);
    }

   public Message pullMessage(int i, int []offsetArray, String bucket) {
       MyFileChannel myfileChannel=null;
       myfileChannel = fileChannelMap.get(bucket);
       DefaultBytesMessage message = null;
       try {
           message = (DefaultBytesMessage) myfileChannel.read(i , offsetArray);
       } catch (EOFException e) {
//           e.printStackTrace();
       }catch (Exception e){
           e.printStackTrace();
       }
       return message;
   }
}

