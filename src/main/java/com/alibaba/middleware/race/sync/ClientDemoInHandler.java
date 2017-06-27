package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.alibaba.middleware.race.sync.Constants.*;

public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {


    private FileChannel fileChannel;

    public ClientDemoInHandler() {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(RESULT_HOME + RESULT_FILE_NAME, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        fileChannel.write(buf.nioBuffer());
        buf.release();


        if (fileChannel.size() == RESULT_SIZE) {
            fileChannel.close();

            System.exit(0);
//            ctx.close();
        }
//        if(first) {
//            ByteBuf byteBuf = ctx.alloc().buffer(4);
//            buf.getBytes(0,byteBuf);
//
//            len = byteBuf.readInt();
//
//            first = false;
//            buf.release();
//            return;
//        }

//        Logger logger = LoggerFactory.getLogger(ClientDemoInHandler.class);
//        logger.info("write to " + RESULT_HOME + RESULT_FILE_NAME);


        //测试输出信息
//        System.out.print(buf.readCharSequence(buf.readableBytes(), Constants.CHARSET));


//        if(fileChannel.size() == len) {
//            fileChannel.close();
//            ctx.close();
//        }
//        fileChannel.close();
//        ctx.close();
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
