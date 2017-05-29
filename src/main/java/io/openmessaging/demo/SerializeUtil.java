package io.openmessaging.demo;

/**
 * Created by Then on 2017/5/24.
 */
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Set;

public class SerializeUtil {
    /**
     * 序列化
     *
     * @param message
     * @return
     */
    public static byte[] serialize(DefaultBytesMessage message) {
//        ObjectOutputStream oos = null;
//        ByteArrayOutputStream baos = null;
        try {
            // 序列化
//            baos = new ByteArrayOutputStream();
//            oos = new ObjectOutputStream(baos);
//            oos.writeObject(object);
//            oos.close();
//            baos.close();
//            byte[] bytes = baos.toByteArray();
//            return bytes;
//            String headers = "";
//            Set<String> set = message.headers().keySet();
//            for (Iterator<String> it = set.iterator(); it.hasNext(); ) {
//                String s = it.next();
//                headers = message.headers().getString(s);
//            }
            StringBuilder header = new StringBuilder();
            KeyValue headerkv = message.headers();
            Set<String> keySet = headerkv.keySet();
            for (String key : keySet) {
                header.append(key);
                header.append("=");
                header.append(headerkv.getString(key));
                header.append("\n");
            }

            StringBuilder properties = new StringBuilder();
            KeyValue propertieskv = message.properties();
            if(propertieskv!=null){
                keySet = propertieskv.keySet();
                for (String key : keySet) {
                    properties.append(key);
                    properties.append("=");
                    properties.append(propertieskv.getString(key));
                    properties.append("\n");
                }
            }
            byte[] headerBytes = header.toString().getBytes();
            int headerLen = headerBytes.length;
            byte []headerLenBytes = intToByteArray(headerLen);
            byte[] propertiesBytes = properties.toString().getBytes();
            int propertiesLen = propertiesBytes.length;
            byte []propertiesLenBytes = intToByteArray(propertiesLen);
            byte []bodyBytes = message.getBody();
            int bodyLen = bodyBytes.length;
            byte []bodyLenBytes = intToByteArray(bodyLen);
            byte[] bytes = new byte[headerBytes.length+4
                    +propertiesBytes.length+4
                    +bodyBytes.length+4];
            int bytesOffset = 0;
            System.arraycopy(headerLenBytes,0,bytes,bytesOffset,headerLenBytes.length);
            bytesOffset+=headerLenBytes.length;
            System.arraycopy(propertiesLenBytes,0,bytes,bytesOffset,propertiesLenBytes.length);
            bytesOffset+=propertiesLenBytes.length;
            System.arraycopy(bodyLenBytes,0,bytes,bytesOffset,bodyLenBytes.length);
            bytesOffset+=bodyLenBytes.length;
            System.arraycopy(headerBytes,0,bytes,bytesOffset,headerBytes.length);
            bytesOffset+=headerBytes.length;
            System.arraycopy(propertiesBytes,0,bytes,bytesOffset,propertiesBytes.length);
            bytesOffset+=propertiesBytes.length;
            System.arraycopy(bodyBytes,0,bytes,bytesOffset,bodyBytes.length);
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 反序列化
     *
     * @param bytes
     * @return
     */
    public static Object unserialize(byte[] bytes) {
//        ByteArrayInputStream bais = null;
//        try {
//            // 反序列化
//            bais = new ByteArrayInputStream(bytes);
//            ObjectInputStream ois = new ObjectInputStream(bais);
//            Object object = ois.readObject();
//            ois.close();
//            bais.close();
//            return object;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
        int bytesOffset = 0;
        //取message各个长度的标志
        byte []headerLenBytes = new byte[4];
        byte []propertiesLenBytes = new byte[4];
        byte []bodyLenBytes = new byte[4];
        System.arraycopy(bytes,bytesOffset,headerLenBytes,0,headerLenBytes.length);
        bytesOffset+=headerLenBytes.length;
        System.arraycopy(bytes,bytesOffset,propertiesLenBytes,0,propertiesLenBytes.length);
        bytesOffset+=propertiesLenBytes.length;
        System.arraycopy(bytes,bytesOffset,bodyLenBytes,0,bodyLenBytes.length);
        bytesOffset+=bodyLenBytes.length;
        int headerLen = byteArrayToInt(headerLenBytes);
        int propertiesLen = byteArrayToInt(propertiesLenBytes);
        int bodyLen = byteArrayToInt(bodyLenBytes);
        //取message实体
        byte []headerBytes = new byte[headerLen];
        byte []propertiesBytes = new byte[propertiesLen];
        byte []bodyBytes = new byte[bodyLen];
        System.arraycopy(bytes,bytesOffset,headerBytes,0,headerLen);
        bytesOffset+=headerLen;
        System.arraycopy(bytes,bytesOffset,propertiesBytes,0,propertiesLen);
        bytesOffset+=propertiesLen;
        System.arraycopy(bytes,bytesOffset,bodyBytes,0,bodyLen);

        //byte数组转成对应的属性
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(bodyBytes);
        String strHeader = new String(headerBytes);
        for ( String kv : strHeader.split("\n") ) {
            String []kvArray=kv.split("=");
            defaultBytesMessage.putHeaders(kvArray[0],kvArray[1]);
        }

        String strProperties = new String(propertiesBytes);
        if(!strProperties.equals("")){
            for ( String kv : strProperties.split("\n") ) {
                String []kvArray=kv.split("=");
                defaultBytesMessage.putProperties(kvArray[0],kvArray[1]);
            }
        }
        return defaultBytesMessage;

//        String message[]=new String(bytes).split("\n");
//        String []header=message[0].split("=");
//        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(message[1].getBytes());
//        defaultBytesMessage.putHeaders(header[0],header[1]);
//        return defaultBytesMessage;
    }

    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        //由高位到低位
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    /**
     * byte[]转int
     *
     * @param bytes
     * @return
     */
    public static int byteArrayToInt(byte[] bytes) {
        int value = 0;
        //由高位到低位
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (bytes[i] & 0x000000FF) << shift;//往高位游
        }
        return value;
    }

    public static  byte[] byteMerger(byte[] byte_1, byte[] byte_2){
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }
}
