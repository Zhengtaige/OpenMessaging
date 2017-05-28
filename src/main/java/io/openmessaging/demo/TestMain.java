package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Producer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by autulin on 2017/5/12.
 */
public class TestMain {
    public static void main(String[] args) throws IOException {
        DefaultBytesMessage message = new DefaultBytesMessage("test".getBytes());
        message.putHeaders("queue", "quueueue");
        message.putHeaders("tttt", 1);
        message.putHeaders("double", 123.123);

        StringBuilder stringBuilder = new StringBuilder();

        Set<String> set = message.headers().keySet();
        for (Iterator<String> it = set.iterator(); it.hasNext(); ) {
            String s = it.next();
            stringBuilder.append(s);
            stringBuilder.append('=');
            stringBuilder.append(message.headers().getString(s));
            stringBuilder.append('\n');
        }
        byte[] header = stringBuilder.toString().getBytes();
        int headerLength = header.length;
        byte[] bytes = byteMerger(header, message.getBody());
        int total = bytes.length;


        DefaultBytesMessage message1 = new DefaultBytesMessage(message.getBody());
        String headers = new String(header);
        for (String s : headers.split("\n")){
            String[] t = s.split("=");
            System.out.println(t[0]+"="+t[1]);

        }

        String s = "123";
        System.out.println((s instanceof String ));
        System.out.println((Object)s instanceof String);




    }

    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2){
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }

    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        //由高位到低位
        result[0] = (byte)((i >> 24) & 0xFF);
        result[1] = (byte)((i >> 16) & 0xFF);
        result[2] = (byte)((i >> 8) & 0xFF);
        result[3] = (byte)(i & 0xFF);
        return result;
    }

    /**
     * byte[]转int
     * @param bytes
     * @return
     */
    public static int byteArrayToInt(byte[] bytes) {
        int value= 0;
        //由高位到低位
        for (int i = 0; i < 4; i++) {
            int shift= (4 - 1 - i) * 8;
            value +=(bytes[i] & 0x000000FF) << shift;//往高位游
        }
        return value;
    }
}
