package com.alibaba.middleware.race.sync;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static com.alibaba.middleware.race.sync.Constants.*;

public class Client {
    private static final int TIME_OUT = 30000;

    public static void main(String[] args) {

        Socket socket = null;
        try {
            System.out.println("客户端启动...");
            socket = new Socket();
            socket.setReceiveBufferSize(128 * 1024);
            socket.setTcpNoDelay(true);
            socket.setPerformancePreferences(0, 0, 2);
            InetSocketAddress address = new InetSocketAddress(args[0], SERVER_PORT);
            //创建一个流套接字并将其连接到指定主机上的指定端口号

            TimeUnit.SECONDS.sleep(3);
            socket.connect(address);

            //读取服务器端数据
            InputStream input = socket.getInputStream();

//            RandomAccessFile randomAccessFile = new RandomAccessFile(RESULT_HOME + RESULT_FILE_NAME, "rw");

            FileOutputStream fileOutputStream = new FileOutputStream(RESULT_HOME + RESULT_FILE_NAME);
//                fileOutputStream.write(data);
//
            byte[] buf = new byte[128 * 1024];
            int len;
            while ((len = input.read(buf)) > 0) {
                fileOutputStream.write(buf, 0, len);
//                    System.out.println(len);
            }

//                byte[] buf = new byte[1024];
//                while(input.read(buf) != -1){
//                    fileOutputStream.write(buf);
//                }

//                byte[] buf = new byte[1024];
//                while(input.read(buf) > 0){
//                    randomAccessFile.write(buf);
//                }

            input.close();
//            randomAccessFile.close();
            fileOutputStream.flush();
            fileOutputStream.close();
            long end = System.currentTimeMillis();
            System.out.println(end);
//            System.exit(0);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    socket = null;
                    System.out.println("客户端 finally 异常:" + e.getMessage());
                }
            }
        }
    }
}