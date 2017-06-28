package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.alibaba.middleware.race.sync.Constants.RESULT_FILE_NAME;
import static com.alibaba.middleware.race.sync.Constants.RESULT_HOME;

public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {


    private FileChannel fileChannel;
    private RandomAccessFile randomAccessFile;

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



        if(fileChannel.size() == 147892188){
            fileChannel.force(false);
            fileChannel.close();
            System.out.println(System.currentTimeMillis());
            System.exit(0);
        }



//        ByteBuf buf = (ByteBuf) msg;

//        ByteArrayInputStream in = new ByteArrayInputStream(buf.array());
//
//        BlockLZ4CompressorInputStream zIn = new BlockLZ4CompressorInputStream(in);
//
//        byte[] data = new byte[150 * 1024 * 1024];
//        zIn.read(data,0,data.length);
//
//        zIn.close();



//        byte[] data = new byte[buf.readableBytes()];
//        buf.readBytes(data);
//
//        LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
//        byte[] val = decompressor.decompress(data,147892188);
//        randomAccessFile.write(val);
//        randomAccessFile.close();
//        System.out.println(System.currentTimeMillis());
//        System.exit(0);




//        if (fileChannel.size() == RESULT_SIZE) {
//            fileChannel.force(false);
//            fileChannel.close();
//            System.exit(0);
////            ctx.close();
//        }
    }

}
