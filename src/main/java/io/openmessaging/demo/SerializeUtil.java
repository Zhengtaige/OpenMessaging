package io.openmessaging.demo;

/**
 * Created by Then on 2017/5/24.
 */

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class SerializeUtil {

    /**
     * 反序列化
     *
     * @param bytes
     * @return
     */
    public static DefaultBytesMessage unserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream bytesStream = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bytesStream);
        return deserialize(in);
    }

    public static DefaultBytesMessage deserialize(DataInputStream in) throws IOException {
        int bodyLength = in.readInt();
        if(bodyLength>20){
            System.out.println(bodyLength);
        }
        byte[] body = new byte[bodyLength];
        in.read(body);
        DefaultBytesMessage message = new DefaultBytesMessage(body);

        int headersNum = in.read();
        int propertiesNum = in.read();

        for (int i = 0; i < headersNum; i++) {
            byte[] keyBytes = new byte[1];
            in.read(keyBytes);
            String key = DefaultBytesMessage.byteToKey(keyBytes[0]);
            int val3Len = in.readShort();
            byte[] val3Bytes = new byte[val3Len];
            in.read(val3Bytes);
            String val3 = new String(val3Bytes);
            message.putMyHeaders(key, val3);
        }

        for (int i = 0; i < propertiesNum; i++) {
            int keyLen = in.readShort();
            byte[] keyBytes = new byte[keyLen];
            in.read(keyBytes);
            String key = new String(keyBytes);
            int val3Len = in.readShort();
            byte[] val3Bytes = new byte[val3Len];
            in.read(val3Bytes);
            String val3 = new String(val3Bytes);
            message.putProperties(key, val3);
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

    public static byte[] shortToByteArray(int i) {
        byte[] result = new byte[2];
        //由高位到低位
        result[0] = (byte) ((i >> 8) & 0xFF);
        result[1] = (byte) (i & 0xFF);
        return result;
    }

}
