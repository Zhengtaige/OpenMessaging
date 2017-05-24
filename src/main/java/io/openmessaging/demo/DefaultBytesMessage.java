package io.openmessaging.demo;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;


import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

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
    @Override public String toString(){
        String ret=new String();
        Set<String> keySet = headers.keySet();
        Iterator<String> iterator=keySet.iterator();
        while(iterator.hasNext()){
            String key=iterator.next();
            ret+=key+"=";
            ret+=headers.getString(key)+"\n";
            //TODO:不能确定value是什么类型的，需要捕捉异常或者实现get方法直接获取Object对象，这里先默认都是String
        }
        ret+="\n";
        Set<String> proptiesSet = properties.keySet();
        Iterator<String> iteratorpro=proptiesSet.iterator();
        while(iteratorpro.hasNext()){
        	String keypro=iteratorpro.next();
        	ret+=keypro+"=";
        	ret+=properties.getString(keypro)+"\n";
        }
        ret+="\n";
        ret+=body.toString();
        return ret;
    }

}
