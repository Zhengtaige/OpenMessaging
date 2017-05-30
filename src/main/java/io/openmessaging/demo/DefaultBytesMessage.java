package io.openmessaging.demo;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;


import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import org.junit.Assert;

public class DefaultBytesMessage implements BytesMessage,Serializable{

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

//    @Override public String toString(){
//        String ret=new String();
//        Set<String> keySet = headers.keySet();
//        Iterator<String> iterator=keySet.iterator();
//        while(iterator.hasNext()){
//            String key=iterator.next();
//            ret+=key+"=";
//            ret+=headers.getString(key)+"\n";
//            //TODO:不能确定value是什么类型的，需要捕捉异常或者实现get方法直接获取Object对象，这里先默认都是String
//        }
//        ret+="\n";
//        Set<String> proptiesSet = properties.keySet();
//        Iterator<String> iteratorpro=proptiesSet.iterator();
//        while(iteratorpro.hasNext()){
//        	String keypro=iteratorpro.next();
//        	ret+=keypro+"=";
//        	ret+=properties.getString(keypro)+"\n";
//        }
//        ret+="\n";
//        ret+=body.toString();
//        return ret;
//    }

    @Override
    public boolean equals(Object obj) {
        DefaultBytesMessage actualMessage = (DefaultBytesMessage) obj;
        KeyValue headerkv = actualMessage.headers();
        Set<String> keySet = headerkv.keySet();
        for (String key : keySet) {
            _equal(this.headers.getString(key), headerkv.getString(key));
        }

        KeyValue propertieskv = actualMessage.properties();
        if(propertieskv!=null){
            keySet = propertieskv.keySet();
            for (String key : keySet) {
                _equal(this.properties.getString(key), propertieskv.getString(key));
            }
        }
//        String topic = actualMessage.headers().getString(MessageHeader.TOPIC);
//        String queue = actualMessage.headers().getString(MessageHeader.QUEUE);
//        String bucket = (topic!=null) ? topic : queue;
//        int num=-1;
//        if(bucket.equals("TOPIC1")){
//            num=DemoTester.topic1Offset;
//        }else if(bucket.equals("TOPIC2")){
//            num=DemoTester.topic2Offset;
//        }else if(bucket.equals("QUEUE1")){
//            num=DemoTester.queue1Offset;
//        } else if(bucket.equals("QUEUE2")){
//            num=DemoTester.queue2Offset;
//        }
//        System.out.println(bucket+":"+num);
        Assert.assertArrayEquals(this.body, actualMessage.getBody());
        return true;
    }

    public void _equal(String expected, String actual){
        if (!expected.equals(actual)) {
            throw new RuntimeException("expected:"+ expected + ", actual:" + actual);
        }
    }
}
