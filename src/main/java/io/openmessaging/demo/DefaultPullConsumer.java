package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    public BlockingQueue<Message> messageQueue = new ArrayBlockingQueue<Message>(1024 * 100);
    public int endedBucketNum;
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        messageStore.setPath(properties.getString("STORE_PATH"));
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public synchronized Message poll() {
        try {
            for (int i = 0; i < bucketList.size(); i++) {
                String bucket = bucketList.get(i);
                    if (((DefaultBytesMessage)(messageStore.pullMessage(queue, bucket))).getBody()==null) {
                        bucketList.remove(i);
                        i--;
                    }
            }
            DefaultBytesMessage message=(DefaultBytesMessage) messageQueue.take();
            while(message.getBody() == null) {
                endedBucketNum--;
                if(endedBucketNum==0){
                  return null;
                }
                message=(DefaultBytesMessage) messageQueue.take();
            }
          return message;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
        endedBucketNum=bucketList.size();
        for (String bucket:
                bucketList) {
            if(!messageStore.bucketConsumerMap.containsKey(bucket)){
                messageStore.bucketConsumerMap.put(bucket, new ConcurrentLinkedQueue<DefaultPullConsumer>());
            }
            ConcurrentLinkedQueue consumerQueue = messageStore.bucketConsumerMap.get(bucket);
            consumerQueue.add(this);
        }

    }
}
