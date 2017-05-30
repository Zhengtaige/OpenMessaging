package io.openmessaging.demo;

/**
 * Created by Then on 2017/5/24.
 */
import io.openmessaging.KeyValue;

import java.io.*;
import java.util.Set;

public class SerializeUtil {
    /**
     * 序列化
     *
     * @param message
     * @return
     */
    public static final int TYPE_INT = 0;
    public static final int TYPE_LONG = 1;
    public static final int TYPE_DOUBLE = 2;
    public static final int TYPE_STRING = 3;
    public static byte[] serialize(DefaultBytesMessage message) {
//        ObjectOutputStream oos = null;
//        ByteArrayOutputStream baos = null;
        try {
            DefaultKeyValue headers = (DefaultKeyValue) message.headers();
            DefaultKeyValue properties = (DefaultKeyValue) message.properties();

            ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytesStream);

            try {
                out.writeInt(message.getBody().length);
                out.write(message.getBody());

                out.writeShort(headers.kvs.size());
                out.writeShort(properties == null ? 0 : properties.kvs.size());

                for (String key : headers.keySet()) {

                    out.writeShort(key.getBytes().length);
                    out.write(key.getBytes());

                    Object val = headers.kvs.get(key);
                    if (val instanceof Integer) {
                        out.writeByte(TYPE_INT);
                        out.writeInt((int) val);
                    } else if (val instanceof Long) {
                        out.writeByte(TYPE_LONG);
                        out.writeLong((long) val);
                    } else if (val instanceof Double) {
                        out.writeByte(TYPE_DOUBLE);
                        out.writeDouble((double) val);
                    } else {
                        // String
                        String v = (String) val;
                        out.writeByte(TYPE_STRING);
                        out.writeShort(v.getBytes().length);
                        out.write(v.getBytes());
                    }
                }

                if (properties != null) {
                    for (String key : properties.keySet()) {

                        out.writeShort(key.getBytes().length);
                        out.write(key.getBytes());

                        Object val = properties.kvs.get(key);
                        if (val instanceof Integer) {
                            out.writeByte(TYPE_INT);
                            out.writeInt((int) val);
                        } else if (val instanceof Long) {
                            out.writeByte(TYPE_LONG);
                            out.writeLong((long) val);
                        } else if (val instanceof Double) {
                            out.writeByte(TYPE_DOUBLE);
                            out.writeDouble((double) val);
                        } else {
                            // String
                            String v = (String) val;
                            out.writeByte(TYPE_STRING);
                            out.writeShort(v.getBytes().length);
                            out.write(v.getBytes());
                        }
                    }
                }
                out.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return bytesStream.toByteArray();
//
//            StringBuilder header = new StringBuilder();
//            KeyValue headerkv = message.headers();
//            Set<String> keySet = headerkv.keySet();
//            for (String key : keySet) {
//                header.append(key);
//                header.append("=");
//                header.append(headerkv.getString(key));
//                header.append("\n");
//            }
//
//            StringBuilder properties = new StringBuilder();
//            KeyValue propertieskv = message.properties();
//            if(propertieskv!=null){
//                keySet = propertieskv.keySet();
//                for (String key : keySet) {
//                    properties.append(key);
//                    properties.append("=");
//                    properties.append(propertieskv.getString(key));
//                    properties.append("\n");
//                }
//            }
//            byte[] headerBytes = header.toString().getBytes();
//            int headerLen = headerBytes.length;
//            byte []headerLenBytes = intToByteArray(headerLen);
//            byte[] propertiesBytes = properties.toString().getBytes();
//            int propertiesLen = propertiesBytes.length;
//            byte []propertiesLenBytes = intToByteArray(propertiesLen);
//            byte []bodyBytes = message.getBody();
//            int messageLen = 4+4
//                    +headerBytes.length
//                    +propertiesBytes.length
//                    +bodyBytes.length;
//            byte []messageLenBytes = intToByteArray(messageLen);
//            byte[] bytes = new byte[messageLen+4];
//            int bytesOffset = 0;
//            System.arraycopy(messageLenBytes,0,bytes,bytesOffset,messageLenBytes.length);
//            bytesOffset+=messageLenBytes.length;
//            System.arraycopy(headerLenBytes,0,bytes,bytesOffset,headerLenBytes.length);
//            bytesOffset+=headerLenBytes.length;
//            System.arraycopy(propertiesLenBytes,0,bytes,bytesOffset,propertiesLenBytes.length);
//            bytesOffset+=propertiesLenBytes.length;
//            System.arraycopy(headerBytes,0,bytes,bytesOffset,headerBytes.length);
//            bytesOffset+=headerBytes.length;
//            System.arraycopy(propertiesBytes,0,bytes,bytesOffset,propertiesBytes.length);
//            bytesOffset+=propertiesBytes.length;
//            System.arraycopy(bodyBytes,0,bytes,bytesOffset,bodyBytes.length);
//            return bytes;
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
//        int bytesOffset = 0;
//        //取message各个长度的标志
//        byte []headerLenBytes = new byte[4];
//        byte []propertiesLenBytes = new byte[4];
//        System.arraycopy(bytes,bytesOffset,headerLenBytes,0,headerLenBytes.length);
//        bytesOffset+=headerLenBytes.length;
//        System.arraycopy(bytes,bytesOffset,propertiesLenBytes,0,propertiesLenBytes.length);
//        bytesOffset+=propertiesLenBytes.length;
//        int headerLen = byteArrayToInt(headerLenBytes);
//        int propertiesLen = byteArrayToInt(propertiesLenBytes);
//        int bodyLen = bytes.length-headerLen-propertiesLen-8;
//        //取message实体
//        byte []headerBytes = new byte[headerLen];
//        byte []propertiesBytes = new byte[propertiesLen];
//        byte []bodyBytes = new byte[bodyLen];
//        System.arraycopy(bytes,bytesOffset,headerBytes,0,headerLen);
//        bytesOffset+=headerLen;
//        System.arraycopy(bytes,bytesOffset,propertiesBytes,0,propertiesLen);
//        bytesOffset+=propertiesLen;
//        System.arraycopy(bytes,bytesOffset,bodyBytes,0,bodyLen);
//
//        //byte数组转成对应的属性
//        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(bodyBytes);
//        String strHeader = new String(headerBytes);
//        for ( String kv : strHeader.split("\n") ) {
//            String []kvArray=kv.split("=");
//            defaultBytesMessage.putHeaders(kvArray[0],kvArray[1]);
//        }
//
//        String strProperties = new String(propertiesBytes);
//        if(!strProperties.equals("")){
//            for ( String kv : strProperties.split("\n") ) {
//                String []kvArray=kv.split("=");
//                defaultBytesMessage.putProperties(kvArray[0],kvArray[1]);
//            }
//        }
//        return defaultBytesMessage;
        ByteArrayInputStream bytesStream = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bytesStream);

        try {
            return deserialize(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static DefaultBytesMessage deserialize(DataInputStream in) throws IOException {
        int bodyLength = in.readInt();
        byte[] body = new byte[bodyLength];
        in.read(body);
        DefaultBytesMessage message = new DefaultBytesMessage(body);

        int headersNum = in.readShort();
        int propertiesNum = in.readShort();

        for (int i = 0; i < headersNum; i++) {
            int keyLen = in.readShort();
            byte[] keyBytes = new byte[keyLen];
            in.read(keyBytes);
            String key = new String(keyBytes);

            int valueType = in.readByte();
            switch(valueType) {
                case TYPE_INT:
                    int val0 = in.readInt();
                    message.putHeaders(key, val0);
                    break;
                case TYPE_LONG:
                    long val1 = in.readLong();
                    message.putHeaders(key, val1);
                    break;
                case TYPE_DOUBLE:
                    double val2 = in.readDouble();
                    message.putHeaders(key, val2);
                    break;
                case TYPE_STRING:
                    int val3Len = in.readShort();
                    byte[] val3Bytes = new byte[val3Len];
                    in.read(val3Bytes);
                    String val3 = new String(val3Bytes);
                    message.putHeaders(key, val3);
                    break;
            }
        }

        for (int i = 0; i < propertiesNum; i++) {
            int keyLen = in.readShort();
            byte[] keyBytes = new byte[keyLen];
            in.read(keyBytes);
            String key = new String(keyBytes);

            int valueType = in.readByte();
            switch(valueType) {
                case TYPE_INT:
                    int val0 = in.readInt();
                    message.putProperties(key, val0);
                    break;
                case TYPE_LONG:
                    long val1 = in.readLong();
                    message.putProperties(key, val1);
                    break;
                case TYPE_DOUBLE:
                    double val2 = in.readDouble();
                    message.putProperties(key, val2);
                    break;
                case TYPE_STRING:
                    int val3Len = in.readShort();
                    byte[] val3Bytes = new byte[val3Len];
                    in.read(val3Bytes);
                    String val3 = new String(val3Bytes);
                    message.putProperties(key, val3);
                    break;
            }
        }

        return message;
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
