package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private String schema;
    private String table;
    private int start;
    private int end;
    private Channel channel;
    private FileChannel fileChannel;

    public ServerDemoInHandler(String schema, String table, int start, int end) {
        this.schema = schema;
        this.table = table;
        this.start = start;
        this.end = end;

//        try {
//            RandomAccessFile randomAccessFile = new RandomAccessFile(MIDDLE_HOME + "insert", "rw");
//            fileChannel = randomAccessFile.getChannel();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }

    }


    /**
     * 根据channel
     *
     * @param ctx
     * @return
     */
//    public static String getIPString(ChannelHandlerContext ctx) {
//        String ipString = "";
//        String socketString = ctx.channel().remoteAddress().toString();
//        int colonAt = socketString.indexOf(":");
//        ipString = socketString.substring(1, colonAt);
//        return ipString;
//    }

//    @Override
//    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//
//        Logger logger = LoggerFactory.getLogger(Server.class);
//
//        logger.info("start fileParser...");
//        FileParser fileParser = new FileParser(schema,table,lo,hi);
//
//        for (int i = 1; i <= 10; i++) {
//            fileParser.readPage((byte) i);
//            logger.info("fileParser has read " + i);
//        }
//        logger.info("start showResult...");
//        fileParser.showResult();
//        logger.info("file has been written to " + Constants.MIDDLE_HOME + RESULT_FILE_NAME);
//
//        String fileName = Constants.MIDDLE_HOME + RESULT_FILE_NAME;
//        RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
//        FileChannel fileChannel = randomAccessFile.getChannel();
//        FileRegion fileRegion = new DefaultFileRegion(fileChannel, 0, fileChannel.size());
//
//        final ChannelFuture future = ctx.writeAndFlush(fileRegion);
//
//        future.addListener(ChannelFutureListener.CLOSE);
//
//    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Logger logger = LoggerFactory.getLogger(Server.class);
        logger.error(cause.getMessage());
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        Server.channel = ctx.channel();
        writeTest(ctx);
    }

    public void writeTest(ChannelHandlerContext ctx) {
        try {

            RandomAccessFile randomAccessFile = new RandomAccessFile("D:\\IncrementalSync\\example\\1.txt", "r");
            byte[] data = new byte[(int) randomAccessFile.length()];
            System.out.println("size : " + data.length);
            randomAccessFile.read(data);
//            ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(150 * 1024 * 1024);
//            buf.writeBytes(data);




//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//
//            BlockLZ4CompressorOutputStream lzOut = new BlockLZ4CompressorOutputStream(out);
//            lzOut.write(data,0,data.length);
//
//            lzOut.close();
//
//            System.out.println(start);
//            ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(150 * 1024 * 1024);
//
//            buf.writeBytes(out.toByteArray());
//            final long[] end = new long[1];
//            ChannelFuture future = Server.channel.writeAndFlush(buf);




            ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(data.length);
            buf.writeBytes(data);

            final long start = System.currentTimeMillis();
            System.out.println(start);

            ChannelFuture future = ctx.writeAndFlush(buf);




//            LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
//            byte[] val = compressor.compress(data);
//            ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(val.length);
//            buf.writeBytes(val);
//            ChannelFuture future = ctx.writeAndFlush(buf);

            final long[] end = new long[1];
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    end[0] = System.currentTimeMillis();
                    System.out.println("cost time: " + (end[0] - start));
//                    channelFuture.channel().close();
                }
            });


//            long start = System.currentTimeMillis();
//            System.out.println(start);
//            Server.channel.writeAndFlush(buf);
//                ChannelFuture future = channel.writeAndFlush(buf);
//                future.addListener(ChannelFutureListener.CLOSE);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //    @Override
//    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        Logger logger = LoggerFactory.getLogger(Server.class);
//        try {
//            String fileName = Constants.MIDDLE_HOME + RESULT_FILE_NAME;
//            RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
//            FileChannel fileChannel = randomAccessFile.getChannel();
//            FileRegion fileRegion = new DefaultFileRegion(fileChannel, 0, fileChannel.size());
//
//            final ChannelFuture future = ctx.writeAndFlush(fileRegion);
//
//            future.addListener(ChannelFutureListener.CLOSE);
//        }catch (IOException e){
//            logger.error("writeFile error",e);
//        }
//    }

    //    @Override
//    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        System.out.println("channelRead");
//        Server.channel = ctx.channel();
//    }

    //    @Override
//    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//
//        // 保存channel
//        Server.getMap().put(getIPString(ctx), ctx.channel());
//
//        logger.info("com.alibaba.middleware.race.sync.ServerDemoInHandler.channelRead");
//
//
//        ByteBuf result = (ByteBuf) msg;
//        byte[] result1 = new byte[result.readableBytes()];
//        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
//        result.readBytes(result1);
//        String resultStr = new String(result1);
//        // 接收并打印客户端的信息
//        System.out.println("netty.Client said:" + resultStr);
//
//        while (true) {
//            // 向客户端发送消息
//            String message = (String) getMessage();
//            if (message != null) {
//                Channel channel = Server.getMap().get("127.0.0.1");
//                ByteBuf byteBuf = Unpooled.wrappedBuffer(message.getBytes());
//                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {
//
//                    @Override
//                    public void operationComplete(ChannelFuture future) throws Exception {
//                        logger.info("Server发送消息成功！");
//                    }
//                });
//
//            }
//        }
//
//    }

//    @Override
//    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        ctx.flush();
//    }

//    private Object getMessage() throws InterruptedException {
//        // 模拟下数据生成，每隔5秒产生一条消息
//        Thread.sleep(5000);
//
//        return "message generated in ServerDemoInHandler";
//
//    }


}
