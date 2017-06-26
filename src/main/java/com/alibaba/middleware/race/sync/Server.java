package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.middleware.race.sync.Constants.RESULT_FILE_NAME;

public class Server {
    private static String schema;
    private static String table;
    private static int start;
    private static int end;
    static Channel channel;

    Logger logger = LoggerFactory.getLogger(Server.class);


    // 保存channel
//    private static Map<String, Channel> map = new ConcurrentHashMap<String, Channel>();
//    // 接收评测程序的三个参数
//    private static String schema;
//    private static Map tableNamePkMap;

//    public static Map<String, Channel> getMap() {
//        return map;
//    }
//
//    public static void setMap(Map<String, Channel> map) {
//        Server.map = map;
//    }


    public static void main(String[] args) throws InterruptedException {
        initProperties();
//        printInput(args);

//        schema = args[0];
//        table = args[1];
//        start = Integer.parseInt(args[2]);
//        end = Integer.parseInt(args[3]);


        Server server = new Server();
//        logger.info("com.alibaba.middleware.race.sync.Server is running....");
        server.startServer(5527);


    }

    /**
     * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
     * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
     * id>100 and id<200
     */
//    private static void printInput(String[] args) {
//
//
//        // 第一个参数是Schema Name
//        logger.info("Schema:" + args[0]);
//        // 第二个参数是Schema Name
//        logger.info("table:" + args[1]);
//        // 第三个参数是start pk Id
//        logger.info("start:" + args[2]);
//        // 第四个参数是end pk Id
//        logger.info("end:" + args[3]);
//
//    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
        System.setProperty("log.name", "server-custom");
    }


    private void startServer(int port) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)

                    .option(ChannelOption.SO_BACKLOG,1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
//                    .option(ChannelOption.SO_TIMEOUT, 6000)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_SNDBUF, 256 * 1024)
                    .childOption(ChannelOption.SO_RCVBUF, 256 * 1024)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 注册handler
                            ch.pipeline().addLast(new ServerDemoInHandler(schema, table, start, end));
                            // ch.pipeline().addLast(new ServerDemoOutHandler());
                        }
                    });
//                    .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024 * 500,1024 * 1024 * 500))

            ChannelFuture f = b.bind(port).sync();

            long parseStart = System.currentTimeMillis();
            try {
                parseFile();
            } catch (Exception e) {
                logger.error("parseFile error", e);
            }

            long parseEnd = System.currentTimeMillis();
            logger.info("parseFile time: " + (parseEnd - parseStart));

//            writeFile();
            f.channel().closeFuture().sync();

        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private void parseFile() {

        logger.info("start fileParser...");
        FileParser2 fileParser = new FileParser2();

        for (int i = 1; i <= 10; i++) {
            fileParser.readPage((byte) i);
            logger.info("fileParser has read " + i);
        }

        logger.info("start showResult...");
        fileParser.showResult();
        logger.info("file has been written to " + Constants.MIDDLE_HOME + RESULT_FILE_NAME);


//        FileParser8 fileParser = new FileParser8();
//        fileParser.readPages();
    }

//    private void writeFile(){
//        try {
//            String fileName = Constants.MIDDLE_HOME + RESULT_FILE_NAME;
//            RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
//            FileChannel fileChannel = randomAccessFile.getChannel();
//            FileRegion fileRegion = new DefaultFileRegion(fileChannel, 0, fileChannel.size());
//
//            final ChannelFuture future2 = channel.writeAndFlush(fileRegion);
//            future2.addListener(ChannelFutureListener.CLOSE);
//        }catch (IOException e){
//            logger.error("writeFile error",e);
//        }
//    }

}
