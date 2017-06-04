package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import org.junit.Assert;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

public class DefaultBytesMessage implements BytesMessage,Serializable{

    public KeyValue headers = new DefaultKeyValue();
    public KeyValue properties;
    public byte[] header = new byte[128];
    public byte[] propertie = new byte[128];
    int headerNum = 0;
    int propertieNum = 0;
    int headerLen = 0;
    int propertyLen = 0;
    public byte[] body;


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
        if (4+key.length() + value.length()+headerLen > header.length) {
            byte[] newBytes = new byte[2+1+value.length()+headerLen];
            System.arraycopy(header,0,newBytes,0,headerLen);
            header = newBytes;
        }
        byte []keyByte = new byte[1];
        keyByte[0]=keyToByte(key);
        if(keyByte[0] != -1) {
            byte[] valueBytes = value.getBytes();
            byte[] valueLen = SerializeUtil.shortToByteArray(valueBytes.length);
            try {
                System.arraycopy(keyByte, 0, header, headerLen, 1);
                headerLen += 1;
                System.arraycopy(valueLen, 0, header, headerLen, valueLen.length);
                headerLen += valueLen.length;
                System.arraycopy(valueBytes, 0, header, headerLen, valueBytes.length);
                headerLen += valueBytes.length;
            } catch (Exception e) {
                System.out.println("key:" + key + ",value:" + value);
            }
            headerNum++;
        }
        return this;
    }

    public void  putMyHeaders(String key, String value) {
        headers.put(key, value);
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

        if (4+key.length()+value.length()+propertyLen > propertie.length) {
            byte[] newBytes = new byte[4+key.length()+value.length()+propertyLen];
            System.arraycopy(propertie,0,newBytes,0,propertie.length);
            propertie = newBytes;
        }
        byte[] keyBytes = key.getBytes();
        byte[] keyLen = SerializeUtil.shortToByteArray(keyBytes.length);
        byte[] valueBytes = value.getBytes();
        byte[] valueLen = SerializeUtil.shortToByteArray(valueBytes.length);
        System.arraycopy(keyLen,0,propertie,propertyLen,keyLen.length);
        propertyLen+=keyLen.length;
        System.arraycopy(keyBytes,0,propertie,propertyLen,keyBytes.length);
        propertyLen+=keyBytes.length;
        System.arraycopy(valueLen,0,propertie,propertyLen,valueLen.length);
        propertyLen+=valueLen.length;
        System.arraycopy(valueBytes,0,propertie,propertyLen,valueBytes.length);
        propertyLen+=valueBytes.length;
        propertieNum++;
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

    public byte[] getBytess() throws IOException {
        int offset = 0;
        byte[] ret = new byte[4+body.length+2+2+headerLen+propertyLen];
        byte[] bodyLen = SerializeUtil.intToByteArray(body.length);
        byte[] hLen = SerializeUtil.shortToByteArray(headerNum);
        byte[] pLen = SerializeUtil.shortToByteArray(propertieNum);
        System.arraycopy(bodyLen,0,ret,offset,bodyLen.length);
        offset+=bodyLen.length;
        System.arraycopy(body,0,ret,offset,body.length);
        offset+=body.length;
        System.arraycopy(hLen,0,ret,offset,hLen.length);
        offset+=hLen.length;
        System.arraycopy(pLen,0,ret,offset,pLen.length);
        offset+=pLen.length;
        System.arraycopy(header,0,ret,offset,this.headerLen);
        offset+=this.headerLen;
        System.arraycopy(propertie,0,ret,offset,this.propertyLen);
        return ret;

    }

    byte keyToByte(String key){
        switch (key){
            case MessageHeader.BORN_HOST:
                return 0;
            case MessageHeader.BORN_TIMESTAMP:
                return 1;
            case MessageHeader.MESSAGE_ID:
                return 2;
            case MessageHeader.PRIORITY:
                return 3;
            case MessageHeader.RELIABILITY:
                return 5;
            case MessageHeader.SCHEDULE_EXPRESSION:
                return 6;
            case MessageHeader.SEARCH_KEY:
                return 7;

            case MessageHeader.SHARDING_KEY:
                return 8;

            case MessageHeader.SHARDING_PARTITION:
                return 9;

            case MessageHeader.START_TIME:
                return 10;

            case MessageHeader.STOP_TIME:
                return 11;

            case MessageHeader.STORE_HOST:
                return 12;

            case MessageHeader.STORE_TIMESTAMP:
                return 13;

            case MessageHeader.TIMEOUT:
                return 14;

            case MessageHeader.TRACE_ID:
                return 15;

        }
        return -1;
    }

    public static String byteToKey(byte keyByte){
        switch (keyByte){
            case 0:
                return MessageHeader.BORN_HOST;
            case 1:
                return MessageHeader.BORN_TIMESTAMP;
            case 2:
                return MessageHeader.MESSAGE_ID;
            case 3:
                return MessageHeader.PRIORITY;
            case 5:
                return MessageHeader.RELIABILITY;
            case 6:
                return MessageHeader.SCHEDULE_EXPRESSION;
            case 7:
                return MessageHeader.SEARCH_KEY;

            case 8:
                return MessageHeader.SHARDING_KEY;

            case 9:
                return MessageHeader.SHARDING_PARTITION;

            case 10:
                return MessageHeader.START_TIME;

            case 11:
                return MessageHeader.STOP_TIME;

            case 12:
                return MessageHeader.STORE_HOST;

            case 13:
                return MessageHeader.STORE_TIMESTAMP;

            case 14:
                return MessageHeader.TIMEOUT;

            case 15:
                return MessageHeader.TRACE_ID;

        }
        return null;
    }
}
