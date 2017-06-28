package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static com.alibaba.middleware.race.sync.Constants.SERVER_PORT;

public class Server {

    public static Socket client;
    public static BufferedOutputStream output;

    private static Logger logger;

    public static void main(String[] args) {

        initProperties();

        new Thread(new Runnable() {
            private Logger logger = LoggerFactory.getLogger(Server.class);

            @Override
            public void run() {
                long parseStart = System.currentTimeMillis();
                logger.info("start fileParser...");
                try {
                    parseFile();
                } catch (Exception e) {
                    logger.error("parseFile error", e);
                }

                long parseEnd = System.currentTimeMillis();
                logger.info("parseFile time: " + (parseEnd - parseStart));
            }
        }).start();


        System.out.println("服务器启动...\n");
        Server server = new Server();
        server.init();
    }

    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
        System.setProperty("log.name", "server-custom");

        logger = LoggerFactory.getLogger(Server.class);
    }


    public void init() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReceiveBufferSize(128 * 1024);
            while (true) {
                // 一旦有堵塞, 则表示服务器与客户端获得了连接
                client = serverSocket.accept();
                output = new BufferedOutputStream(client.getOutputStream());
                logger.info("client connected...");
                // 处理这次连接
//                    new HandlerThread(client);
            }
        } catch (Exception e) {
            System.out.println("服务器异常: " + e.getMessage());
        }
    }


    private static void parseFile() {

        /*FileParser2 fileParser = new FileParser2();
        Logger logger = LoggerFactory.getLogger(Server.class);

        for (int i = 1; i <= 10; i++) {
            fileParser.readPage((byte) i);
            logger.info("fileParser has read " + i);
        }

        logger.info("start showResult...");
        fileParser.showResult();*/


        FileParser8 fileParser = new FileParser8();
        fileParser.readPages();
    }


    public static void writeToClient(byte[] data) {

        try {
            logger.info("write start : " + System.currentTimeMillis());
            logger.info("result size : " + data.length);
            output.write(data);
            output.flush();
            output.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


//    private class HandlerThread implements Runnable {
//        private Socket socket;
//        public HandlerThread(Socket client) {
//            socket = client;
//            try {
//                socket.setTcpNoDelay(true);
//                socket.setSendBufferSize(128 * 1024);
//                socket.setPerformancePreferences(0,0,2);
//
//            } catch (SocketException e) {
//                e.printStackTrace();
//            }
//            new Thread(this).start();
//        }
//
//        public void run() {
//            try {
//
//
//                RandomAccessFile randomAccessFile = new RandomAccessFile("E:\\Major\\IncrementalSync\\example\\1.txt","r");
//                byte[] data = new byte[147892188];
//                randomAccessFile.read(data);
//
//
//                long start = System.currentTimeMillis();
//                System.out.println(start);
//                BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
//
//                output.write(data);
//                output.flush();
//                output.close();
//
//            } catch (Exception e) {
//                System.out.println("服务器 run 异常: " + e.getMessage());
//            } finally {
//                if (socket != null) {
//                    try {
//                        socket.close();
//                    } catch (Exception e) {
//                        socket = null;
//                        System.out.println("服务端 finally 异常:" + e.getMessage());
//                    }
//                }
//            }
//        }
//    }


}







