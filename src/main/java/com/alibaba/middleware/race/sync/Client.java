package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

    private final static int port = Constants.SERVER_PORT;
    // idle时间
    private static String ip;
//    private EventLoopGroup loop = new NioEventLoopGroup();
    private static Logger logger = LoggerFactory.getLogger(Client.class);


    public static void main(String[] args) throws Exception {
        initProperties();
        // 从args获取server端的ip
        ip = args[0];
        Client client = new Client();
        client.connect(ip, port);
        logger.info("关闭连接");

    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
        System.setProperty("log.name", "client-custom");
    }

    /**
     * 连接服务端
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .option(ChannelOption.SO_SNDBUF, 256 * 1024)
                    .option(ChannelOption.SO_RCVBUF, 256 * 1024);
//                                .option(ChannelOption.RCVBUF_ALLOCATOR, new DefaultMaxBytesRecvByteBufAllocator());
//                                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(40 * 1024 * 1024));

            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
//                    ch.pipeline().addLast(new IdleStateHandler(10, 0, 0));
//                    ch.pipeline().addLast(new ClientIdleEventHandler());
//                    ch.pipeline().addLast("decoder", new LengthFieldBasedFrameDecoder(1024 * 1024 * 1024,0,4,0,4));
                    ch.pipeline().addLast(new ClientDemoInHandler());
                }
            });

            // Start the client.
            ChannelFuture f = null;
            while(true) {
                try {
                    f = b.connect(host, port).sync();
                    break;
                }catch (Exception e){
                }
            }
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();

        } finally {
            workerGroup.shutdownGracefully();
        }





    }


}
