package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.*;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;


//parse7并发版
public class FileParser8 {
    //    private static final int BLOCK_SIZE = 200 * 1024 * 1024;
    private String schema = SCHEMA;
    private String table = TABLE;
    private int lo = LO;
    private int hi = HI;
    private Logger logger = LoggerFactory.getLogger(Server.class);


    //        private HashMap<Integer, byte[]> resultMap = new HashMap<>();
//    private KMap<Long, byte[]> resultMap = KMap.withExpectedSize();
//    private HashIntObjMap<byte[]> resultMap = HashIntObjMaps.newMutableMap(5000000);
//    private ConcurrentHashMap<Integer, byte[]> resultMap = new ConcurrentHashMap<>(8 * 1024 * 1024);
//    private ConcurrentHashMap<Integer, byte[]> resultMap = new ConcurrentHashMap<>(8 * 1024 * 1024);
//    private HashMap<Integer, byte[]> resultMap = new HashMap<>(8 * 1024 * 1024);
    private HashIntObjMap<byte[]> resultMap = HashIntObjMaps.newMutableMap(8 * 1024 * 1024);
    private boolean mergeResultStart;
    private boolean readFinish;
//    private int updateTotal;
//    private int pkchangeTotal;
//    private int deleteTotal;


    public FileParser8() {
        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");

        executorService.execute(new Runnable() {
            @Override
            public void run() {
//                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                mergeResult();
            }
        });

    }

    //    private BlockingQueue<Future<Result>> futureList = new LinkedBlockingQueue<>(THREAD_NUM);
    private BlockingQueue<Future<Result>> futureList = new LinkedBlockingQueue<>(THREAD_NUM);
    //    private BlockingQueue<Result[]> resultsList = new LinkedBlockingQueue<>();
//    private ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
    private ExecutorService executorService = Executors.newCachedThreadPool();

    public void readPages() {
        try {
            for (int i = 1; i <= 10; i++) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(DATA_HOME + i + ".txt", "r");
                FileChannel fileChannel = randomAccessFile.getChannel();

                int mp = 0;
                MappedByteBuffer mappedByteBuffer;
                int block = DIRECT_CACHE / THREAD_NUM;
                long size = fileChannel.size();
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
                        futureList.put(executorService.submit(new Task3(resultMap, mappedByteBuffer, limit)));
                    } else {
                        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mp, rest);
                        limit = (int) rest;
                        mp = (int) size;
                        futureList.put(executorService.submit(new Task3(resultMap, mappedByteBuffer, limit)));
                    }

//                    //等待任务完成
//                    if(futureList.size() == THREAD_NUM) {
//                        final Result[] results = new Result[THREAD_NUM];
//                        try {
//                            for (int j = 0; j < THREAD_NUM; j++) {
//                                results[j] = futureList.poll().get();
//                            }
//                            resultsList.add(results);
//                        } catch (InterruptedException | ExecutionException e) {
//                            e.printStackTrace();
//                        }
//                    }


//                    int block = limit / THREAD_NUM;
//                    byte[] data;
//                    while(mappedByteBuffer.position() < limit){
//                        data = new byte[block + 200];
//                        mappedByteBuffer.get(data, 0, block);
//                        byte b;
//                        int len = block;
//                        while((b = mappedByteBuffer.get()) != EN){
//                            data[len++] = b;
//                        }
//                        data[len++] = b;
//                        futureList.put(executorService.submit(new Task(data, 0, len)));
//
//                        int res = limit - mappedByteBuffer.position();
//                        if(res < block){
//                            data = new byte[res];
//                            mappedByteBuffer.get(data);
//                            futureList.put(executorService.submit(new Task(data, 0, res)));
//                            break;
//                        }
//                    }


//                while (!readOne) {
//
//                    byte[] data;
//                    int actualLen;
//
//                    long rest = randomAccessFile.length() - randomAccessFile.getFilePointer();
//                    if (rest < CACHE_SIZE) {
//                        data = new byte[(int) rest];
//                        randomAccessFile.read(data);
//                        actualLen = (int) rest;
//                        readOne = true;
//                    } else {
//                        data = new byte[CACHE_SIZE + 200];
//                        randomAccessFile.read(data, 0, CACHE_SIZE);
//                        byte b;
//                        actualLen = CACHE_SIZE;
//                        while ((b = randomAccessFile.readByte()) != EN) {
//                            data[actualLen++] = b;
//                        }
//                        data[actualLen++] = b;
//                    }


//                    int index = 0;
//                    int block = actualLen / THREAD_NUM;
//                    while (true) {
//                        int start = index;
//                        index += block;
//                        while (data[index++] != EN) {
//                        }
//                        int len = index - start;
//                        futureList.put(executorService.submit(new Task(data, start, len)));
//
//                        if (actualLen - index < block) {
//                            futureList.put(executorService.submit(new Task(data, index, actualLen - index)));
//                            break;
//                        }
//                    }

//                    mergeResult();

                }

                logger.info("fileParser has read " + i);

            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

//        //处理余下任务
//        int n = futureList.size();
//        final Result[] results = new Result[n];
//        try {
//            for (int j = 0; j < n; j++) {
//                results[j] = futureList.poll().get();
//            }
//            resultsList.add(results);
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }


        readFinish = true;

    }


    public void mergeResult() {

//        HashIntObjMap<byte[]> insertMapAll = HashIntObjMaps.newMutableMap();
//        LinkedHashMap<Integer, byte[]> updateMapAll = new LinkedHashMap<>();
//        HashIntSet deleteSetAll = HashIntSets.newMutableSet();

//        int n = futureList.size();
//        Result[] results = new Result[n];
//
//        try {
//            for (int i = 0; i < n; i++) {
//                results[i] = futureList.poll().get();
//            }
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }


        Result result = null;
        while (true) {
            try {
                if (!readFinish) {
                    result = futureList.take().get();
                } else {
                    Future<Result> future = futureList.poll();
                    if (future == null) {
                        break;
                    }
                    result = future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

//            HashIntObjMap<byte[]> insertMap = result.getInsertMap();
//            LinkedHashMap<Integer, byte[]> updateMap = result.getUpdateMap();
//            LinkedList<Integer> deleteList = result.getDeleteSet();
//            LinkedHashMap<Integer, Integer> PKchangeMap = result.getPKChangeMap();


            HashIntObjMap<byte[]> updateMap = result.getUpdateMap();
            ArrayList<Integer> updateList = result.getUpdateList();
            ArrayList<Integer> oldPKList = result.getOldPKList();
            ArrayList<Integer> newPKList = result.getNewPKList();
            ArrayList<Integer> deleteList = result.getDeleteList();

//            updateTotal += updateList.size();
//            pkchangeTotal += oldPKList.size();
//            deleteTotal += deleteList.size();


            //先处理PK变更
//            for (Map.Entry<Integer, Integer> entry : PKchangeMap.entrySet()) {
//                int pk = entry.getKey();
//                int newPk = entry.getValue();
//
//                resultMap.put(newPk, resultMap.get(pk));
//                resultMap.remove(pk);
//            }

            int n = oldPKList.size();
            for (int i = 0; i < n; i++) {
                int pk = oldPKList.get(i);
                int newPk = newPKList.get(i);
                resultMap.put(newPk, resultMap.get(pk));
                resultMap.remove(pk);
            }


            //处理update
//            resultMap.putAll(insertMap);


//            for (Map.Entry<Integer, byte[]> entry : updateMap.entrySet()) {
//                int pk = entry.getKey();
//                byte[] update = entry.getValue();
//                byte[] record = resultMap.get(pk);
//
//
//                for (int j = 0; j < KEY_NUM; j++) {
//                    int offset = VAL_OFFSET_ARRAY[j];
//                    if (update[offset] == 0) {
//                        continue;
//                    }
//                    int len = VAL_LEN_ARRAY[j];
//                    System.arraycopy(update, offset, record, offset, len);
//                }
//            }


            n = updateList.size();
            for (int i = 0; i < n; i++) {
                int pk = updateList.get(i);
                byte[] record = resultMap.get(pk);
                if (record == null) {
                    continue;
                }
                byte[] update = updateMap.get(pk);

                for (int j = 0; j < KEY_NUM; j++) {
                    int offset = VAL_OFFSET_ARRAY[j];
                    if (update[offset] == 0) {
                        continue;
                    }
                    int len = VAL_LEN_ARRAY[j];
                    System.arraycopy(update, offset, record, offset, len);
                }
            }


            //处理delete
//            while (!deleteList.isEmpty()) {
//                int pk = deleteList.poll();
//                resultMap.remove(pk);
//            }

            n = deleteList.size();
            for (int i = 0; i < n; i++) {
                int pk = deleteList.get(i);
                resultMap.remove(pk);
            }

        }

//        logger.info("updateTotal: " + updateTotal);
//        logger.info("pkchangeTotal: " + pkchangeTotal);
//        logger.info("deleteTotal: " + deleteTotal);

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


//        //单线程版
//        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(RESULT_BUF);
//        int size = pkList.size();
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


        //多线程版
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

        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);

        Server.writeToClient(data);

//        Server.channel.flush();

//        logger.info("result 大小： " + buf.readableBytes());
//        logger.info("result 大小： " + resultSize);

//        ChannelFuture future = Server.channel.writeAndFlush(buf);
//        future.addListener(ChannelFutureListener.CLOSE);

//        Server.channel.writeAndFlush(buf);
    }

    private class WriteResult implements Callable<ByteBuf> {
//        private ConcurrentHashMap<Integer, byte[]> resultMap;
        private ArrayList<Integer> pkList;
        private int start;
        private int lim;
        private HashIntObjMap<byte[]> resultMap;

//        public WriteResult(ConcurrentHashMap<Integer, byte[]> resultMap, ArrayList<Integer> pkList, int start, int lim) {
//            this.resultMap = resultMap;
//            this.pkList = pkList;
//            this.start = start;
//            this.lim = lim;
//        }
        public WriteResult(HashIntObjMap<byte[]> resultMap, ArrayList<Integer> pkList, int start, int lim) {
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
                    buf.writeByte(CHAR_TABLE);
                    int offset = VAL_OFFSET_ARRAY[i];
                    int len = VAL_LEN_ARRAY[i];
                    byte b;
                    int n = 0;
                    while ((n++ < len) && ((b = record[offset++]) != 0)) {
                        buf.writeByte(b);
                    }
                }
                buf.writeByte(CHAR_ENTER);
            }

            return buf;
        }
    }


}
