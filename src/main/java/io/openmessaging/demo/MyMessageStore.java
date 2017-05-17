package io.openmessaging.demo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class MyMessageStore {
    private static final MyMessageStore INSTANCE = new MyMessageStore();

    public static MyMessageStore getInstance(){
        return INSTANCE;
    }

    private Map<String, Integer> offsets = new HashMap<>();

    private Path path;

    public void putMessage(String topic, String queue, byte[] body) {
        int i;
        if (topic != null) {
            path = Paths.get("/test").resolve("topic").resolve(topic);
            i = getIndex(topic);
        } else {
            path = Paths.get("/test").resolve("queue").resolve(queue);
            i = getIndex(queue);
        }

        try{
            if (Files.notExists(path)){
                Files.createDirectories(path);
            }
            Files.write(path.resolve(String.valueOf(i)), body);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized Integer getIndex(String tOrQ) {
        if (!offsets.containsKey(tOrQ)) {
            offsets.put(tOrQ, 1);
            return 0;
        }
        Integer i = offsets.get(tOrQ);
        offsets.put(tOrQ, i+1);
        return i;
    }
}
