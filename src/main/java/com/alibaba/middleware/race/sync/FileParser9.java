package com.alibaba.middleware.race.sync;/*
package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;


//对应task4,全并发
public class FileParser9 {
    //    private static final int BLOCK_SIZE = 200 * 1024 * 1024;
    private String schema = SCHEMA;
    private String table = TABLE;
    private int lo = LO;
    private int hi = HI;
    private Logger logger = LoggerFactory.getLogger(Server.class);


    //        private HashMap<Integer, byte[]> resultMap = new HashMap<>();
//    private KMap<Long, byte[]> resultMap = KMap.withExpectedSize();
//    private HashIntObjMap<byte[]> resultMap = HashIntObjMaps.newMutableMap(5000000);
    private ConcurrentHashMap<Integer, byte[]> resultMap = new ConcurrentHashMap<>(8 * 1024 * 1024);
    private boolean mergeResultStart;
    private boolean readFinish;
    private int updateTotal;
    private int pkchangeTotal;
    private int deleteTotal;


    public FileParser9() {
        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");

//        executorService.execute(new Runnable() {
//            @Override
//            public void run() {
////                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
//                mergeResult();
//            }
//        });

    }

    //    private BlockingQueue<Future<Result>> futureList = new LinkedBlockingQueue<>(THREAD_NUM);
    private BlockingQueue<Future<?>> futureList = new LinkedBlockingQueue<>(THREAD_NUM);
    //    private BlockingQueue<Result[]> resultsList = new LinkedBlockingQueue<>();
//    private ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
    private ExecutorService executorService = Executors.newCachedThreadPool();

    private Set<Integer> deleteSet = Collections.synchronizedSet(new HashSet<Integer>());

    public void readPages() {
        try {
            for (int i = 1; i <= 10; i++) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(DATA_HOME + i + ".txt", "r");
                FileChannel fileChannel = randomAccessFile.getChannel();

                int mp = 0;
                MappedByteBuffer mappedByteBuffer;
                int block = DIRECT_CACHE / THREAD_NUM;
                long size = fileChannel.size();
                //PKChange针对优化
                if (i == 10) {
                    size = PK_CHANGE_START;
                }
                while (mp < size) {
                    long rest = fileChannel.size() - mp;
                    int limit;
                    if (rest >= block) {
                        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mp, block);
                        mappedByteBuffer.mark();
                        mappedByteBuffer.position(block - 200);
                        while (mappedByteBuffer.get() != EN) {
                        }
                        limit = mappedByteBuffer.position();
                        mp += limit;
                        mappedByteBuffer.reset();
                        futureList.put(executorService.submit(new Task4(resultMap, deleteSet, mappedByteBuffer, limit)));
                    } else {
                        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mp, rest);
                        limit = (int) rest;
                        mp = (int) size;
                        futureList.put(executorService.submit(new Task4(resultMap, deleteSet, mappedByteBuffer, limit)));

                        //PKChange针对优化
                        if (i == 10) {
                            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, PK_CHANGE_START, size - PK_CHANGE_START);
                            limit = (int) (size - PK_CHANGE_START);
                            mp = (int) size;
                            futureList.put(executorService.submit(new Task4(resultMap, deleteSet, mappedByteBuffer, limit)));
                        }
                    }



//                    等待任务完成
                    if(futureList.size() == THREAD_NUM) {
                        try {
                            for (int j = 0; j < THREAD_NUM; j++) {
                                futureList.poll().get();
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }


                }

                logger.info("fileParser has read " + i);

            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        //处理余下任务
        int n = futureList.size();
        try {
            for (int j = 0; j < n; j++) {
                futureList.poll().get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        showResult();

        readFinish = true;

    }


    public void mergeResult() {


        Result result = null;
        while (true) {
            try {
                if (!readFinish) {
                    futureList.take().get();
                } else {
                    Future<?> future = futureList.poll();
                    if (future == null) {
                        break;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        showResult();
    }


    public void showResult() {

        ArrayList<Integer> pkList = new ArrayList<>(2 * 1024 * 1024);
        for (int l : resultMap.keySet()) {
            if (l >= hi || l <= lo) {
                continue;
            }
            pkList.add(l);
        }
        Collections.sort(pkList);
        logger.info("pkList size: " + pkList.size());


        long start = System.currentTimeMillis();

//        for (int j = 0; j < size; j++) {
//            int pk = pkList.get(j);
//            byte[] record = resultMap.get(pk);
//            buf.writeBytes(String.valueOf(pk).getBytes());
////            channel.write(String.valueOf(pk).getBytes());
//            for (int i = 0; i < KEY_NUM; i++) {
//                buf.writeByte('\t');
////                channel.write('\t');
//                int offset = VAL_OFFSET_ARRAY[i];
//                int len = VAL_LEN_ARRAY[i];
////                int len = record[offset];
////                buf.writeBytes(record, offset + 1, len);
//                byte b;
//                int n = 0;
//                while ((n++ < len) && ((b = record[offset++]) != 0)) {
//                    buf.writeByte(b);
////                    channel.write(b);
//                }
//            }
//            buf.writeByte('\n');
////            channel.write('\n');
//        }

        int size = pkList.size();
        int par = size / THREAD_NUM;
        LinkedList<Future<ByteBuf>> bufList = new LinkedList<>();

        for (int i = 0; i < THREAD_NUM - 1; i++) {
            bufList.add(executorService.submit(new WriteResult(resultMap, pkList, par * i, par * (i + 1))));
        }
        bufList.add(executorService.submit(new WriteResult(resultMap, pkList, par * (THREAD_NUM - 1), size)));

        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(RESULT_BUF);

        while (!bufList.isEmpty()) {
            try {
                ByteBuf byteBuf = bufList.poll().get();
                buf.writeBytes(byteBuf);

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }


        long end = System.currentTimeMillis();

        logger.info("构建bytebuf时间 " + (end - start));

        logger.info("result 大小： " + buf.readableBytes());


        ChannelFuture future = Server.channel.writeAndFlush(buf);
        future.addListener(ChannelFutureListener.CLOSE);


    }

    private class WriteResult implements Callable<ByteBuf> {
        private ConcurrentHashMap<Integer, byte[]> resultMap;
        private ArrayList<Integer> pkList;
        private int start;
        private int lim;

        public WriteResult(ConcurrentHashMap<Integer, byte[]> resultMap, ArrayList<Integer> pkList, int start, int lim) {
            this.resultMap = resultMap;
            this.pkList = pkList;
            this.start = start;
            this.lim = lim;
        }

        @Override
        public ByteBuf call() throws Exception {

            ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(RESULT_BUF / THREAD_NUM);

            for (int j = start; j < lim; j++) {
                int pk = pkList.get(j);
                byte[] record = resultMap.get(pk);
                buf.writeBytes(String.valueOf(pk).getBytes());
                for (int i = 0; i < KEY_NUM; i++) {
                    buf.writeByte('\t');
                    int offset = VAL_OFFSET_ARRAY[i];
                    int len = VAL_LEN_ARRAY[i];
                    byte b;
                    int n = 0;
                    while ((n++ < len) && ((b = record[offset++]) != 0)) {
                        buf.writeByte(b);
                    }
                }
                buf.writeByte('\n');
            }

            return buf;
        }
    }


}
*/
