package com.alibaba.middleware.race.sync;

import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.alibaba.middleware.race.sync.Constants.RESULT_FILE_NAME;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private String schema;
    private String table;
    private int lo;
    private int hi;

    public ServerDemoInHandler(String schema, String table, int lo, int hi) {
        this.schema = schema;
        this.table = table;
        this.lo = lo;
        this.hi = hi;
    }

    private static Logger logger = LoggerFactory.getLogger(ServerDemoInHandler.class);

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

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        logger.info("start fileParser...");
        FileParser fileParser = new FileParser(schema,table,lo,hi);

        for (int i = 1; i <= 10; i++) {
            fileParser.readPage((byte) i);
            logger.info("fileParser has read " + i);
        }
        logger.info("start showResult...");
        fileParser.showResult();
        logger.info("file has been written to " + Constants.MIDDLE_HOME + RESULT_FILE_NAME);

        String fileName = Constants.MIDDLE_HOME + RESULT_FILE_NAME;
        RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
        FileChannel fileChannel = randomAccessFile.getChannel();
        FileRegion fileRegion = new DefaultFileRegion(fileChannel, 0, fileChannel.size());

        final ChannelFuture future = ctx.writeAndFlush(fileRegion);
        future.addListener(ChannelFutureListener.CLOSE);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error(cause.getMessage());
    }


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
//        System.out.println("com.alibaba.middleware.race.sync.Client said:" + resultStr);
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
