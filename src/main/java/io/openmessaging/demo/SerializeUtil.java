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
            String topic,queue;
            topic = message.headers().getString(MessageHeader.TOPIC);
            queue = message.headers().getString(MessageHeader.QUEUE);
            String header = ((topic!=null) ? MessageHeader.TOPIC+"="+topic : MessageHeader.QUEUE+"="+queue)+"\n";
            byte[] headerByte = header.getBytes();
            byte[] bytes = byteMerger(headerByte, message.getBody());
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
        String message[]=new String(bytes).split("\n");
        String []header=message[0].split("=");
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(message[1].getBytes());
        defaultBytesMessage.putHeaders(header[0],header[1]);
        return defaultBytesMessage;
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
