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
    private RandomAccessFile randomAccessFile;
    private ByteBuf temp;

    public ClientDemoInHandler() {
        try {
            randomAccessFile = new RandomAccessFile(RESULT_HOME + RESULT_FILE_NAME, "rw");
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
            fileChannel.force(false);
            fileChannel.close();
            System.exit(0);
//            ctx.close();
        }
    }

}
