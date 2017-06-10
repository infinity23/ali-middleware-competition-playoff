package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.alibaba.middleware.race.sync.Constants.RESULT_FILE_NAME;
import static com.alibaba.middleware.race.sync.Constants.RESULT_HOME;

/**
 * Created by wanshao on 2017/5/25.
 */
public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ClientDemoInHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelRead");

        ByteBuf buf = (ByteBuf) msg;
        String fileName = RESULT_HOME + RESULT_FILE_NAME;
        FileChannel fileChannel = new RandomAccessFile(fileName,"rw").getChannel();
        fileChannel.read(buf.nioBuffer());

        ctx.close();
        buf.release();
    }

    // 连接成功后，向server发送消息
//    @Override
//    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelActive");
//        String msg = "I am prepared to receive messages";
//        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
//        encoded.writeBytes(msg.getBytes());
//        ctx.write(encoded);
//        ctx.flush();
//    }
}
