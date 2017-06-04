package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private List<String> bucketList = new ArrayList<>();
    private int[] offsetArray;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public synchronized Message poll() {

            for (int i = 0; i < bucketList.size(); i++) {

                    if(offsetArray[i] == -1){
                        continue;
                    }
                    Message message = null;
                    message = messageStore.pullMessage(i, offsetArray, bucketList.get(i));
                    if (message != null) {
                        if(i == 0){
                            message.putHeaders(MessageHeader.QUEUE,bucketList.get(i));
                        }else{
                            message.putHeaders(MessageHeader.TOPIC,bucketList.get(i));
                        }
                        return message;
                    }else{
                        offsetArray[i]=-1;
                    }

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
        queue = queueName;
        bucketList.add(queueName);
        bucketList.addAll(topics);
        offsetArray = new int[bucketList.size()];
    }
}
