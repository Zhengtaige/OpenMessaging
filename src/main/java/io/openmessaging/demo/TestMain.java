package io.openmessaging.demo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by autulin on 2017/5/12.
 */
public class TestMain {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("/test");
        Files.createDirectories(path);
        Runnable runnable = () -> {
            while (true) {
                int t = getNewInt();
                if (t > 50000) {
                    System.out.println("end: "+System.currentTimeMillis()+ Thread.currentThread().getName());
                    return;
                }
                try {
                    Files.write(Paths.get("/test/"+t), new byte[]{(byte) i});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        System.out.println("start: "+System.currentTimeMillis());
        for (int i = 0; i < 8; i++) {
            new Thread(runnable).start();
        }

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        i = 0;
        runnable = () -> {
            byte[] bytes;
            while (true) {
                int t = getNewInt();
                if (t > 50000) {
                    System.out.println("end: "+System.currentTimeMillis()+ Thread.currentThread().getName());
                    return;
                }
                try {
                    bytes = Files.readAllBytes(Paths.get("/test/"+t));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        };
        System.out.println("start: "+System.currentTimeMillis());
        for (int i = 0; i < 8; i++) {
            new Thread(runnable).start();
        }

    }

    public static  int i = 0;
    public static synchronized int getNewInt(){
        return i++;
    }

}
