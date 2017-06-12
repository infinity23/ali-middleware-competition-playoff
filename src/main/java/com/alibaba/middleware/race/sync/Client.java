package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

    private final static int port = Constants.SERVER_PORT;
    // idle时间
    private static String ip;
    private EventLoopGroup loop = new NioEventLoopGroup();
    private static Logger logger = LoggerFactory.getLogger(Client.class);


    public static void main(String[] args) throws Exception {
        initProperties();
        // 从args获取server端的ip
        ip = args[0];
        Client client = new Client();
        client.connect(ip, port);
        logger.info("client connect...");
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

        long start = System.currentTimeMillis();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, new DefaultMaxBytesRecvByteBufAllocator());
            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
//                    ch.pipeline().addLast(new IdleStateHandler(10, 0, 0));
//                    ch.pipeline().addLast(new ClientIdleEventHandler());
                    ch.pipeline().addLast(new ClientDemoInHandler());
                }
            });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();

        } finally {
            workerGroup.shutdownGracefully();
        }

        long end = System.currentTimeMillis();

        System.out.println("client 运行时间： " + (end - start));

    }


}
