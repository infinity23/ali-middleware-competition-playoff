package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.middleware.race.sync.Cons.VAL_LEN_ARRAY;
import static com.alibaba.middleware.race.sync.Cons.VAL_OFFSET_ARRAY;
import static com.alibaba.middleware.race.sync.Constants.*;


//多线程正读
public class FileParser5 {
    //    private static final int BLOCK_SIZE = 200 * 1024 * 1024;
    private String schema = SCHEMA;
    private String table = TABLE;
    private int lo = LO;
    private int hi = HI;
    private Logger logger = LoggerFactory.getLogger(Server.class);


    //        private HashMap<Integer, byte[]> resultMap = new HashMap<>();
//    private KMap<Long, byte[]> resultMap = KMap.withExpectedSize();
    private HashIntObjMap<byte[]> resultMap = HashIntObjMaps.newMutableMap();
    private boolean mergeResultStart;
    private boolean readFinish;

    public FileParser5() {
        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                mergeResult();
            }
        });

    }


    private BlockingQueue<Future<Result>> futureList = new LinkedBlockingQueue<>(THREAD_NUM);
    private ExecutorService executorService = Executors.newCachedThreadPool();

    public void readPages() {
        try {
            for (int i = 1; i <= 10; i++) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(DATA_HOME + i + ".txt", "r");
                FileChannel fileChannel = randomAccessFile.getChannel();

                int MP = 0;
                boolean readOne = false;
                while (!readOne) {
//                    MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, MP, DIRECT_CACHE - 200);
//                    byte[] data = new byte[DIRECT_CACHE - 200];
//                    mappedByteBuffer.mark();
//                    mappedByteBuffer.position(199 * 1024 * 1024);
//                    while (mappedByteBuffer.get() != EN) {
//                    }
//                    int mlen = mappedByteBuffer.position() - MP;
//
//                    MP += mappedByteBuffer.position();
//
//                    byte[] data = new byte[mlen];
//                    mappedByteBuffer.reset();
//                    mappedByteBuffer.get(data);


                    byte[] data;
                    int actualLen;

                    long rest = randomAccessFile.length() - randomAccessFile.getFilePointer();
                    if (rest < CACHE_SIZE) {
                        data = new byte[(int) rest];
                        randomAccessFile.read(data);
                        actualLen = (int) rest;
                        readOne = true;
                    } else {
                        data = new byte[CACHE_SIZE + 200];
                        randomAccessFile.read(data, 0, CACHE_SIZE);
                        byte b;
                        actualLen = CACHE_SIZE;
                        while ((b = randomAccessFile.readByte()) != EN) {
                            data[actualLen++] = b;
                        }
                        data[actualLen++] = b;
                    }


                    int index = 0;
                    int block = actualLen / THREAD_NUM;
                    while (true) {
                        int start = index;
                        index += block;
                        while (data[index++] != EN) {
                        }
                        int len = index - start;
                        futureList.put(executorService.submit(new Task(data, start, len)));

                        if (actualLen - index < block) {
                            futureList.put(executorService.submit(new Task(data, index, actualLen - index)));
                            break;
                        }
                    }

                }

                logger.info("fileParser has read " + i);

            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

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

            HashIntObjMap<byte[]> insertMap = result.getInsertMap();
            LinkedHashMap<Integer, byte[]> updateMap = result.getUpdateMap();
            LinkedList<Integer> deleteList = result.getDeleteSet();
            LinkedHashMap<Integer, Integer> PKchangeMap = result.getPKChangeMap();

            //先处理PK变更
            for (Map.Entry<Integer, Integer> entry : PKchangeMap.entrySet()) {
                int pk = entry.getKey();
                int newPk = entry.getValue();

                resultMap.put(newPk, resultMap.get(pk));
                resultMap.remove(pk);
            }


            //处理update
            resultMap.putAll(insertMap);
            for (Map.Entry<Integer, byte[]> entry : updateMap.entrySet()) {
                int pk = entry.getKey();
                byte[] update = entry.getValue();
                byte[] record = resultMap.get(pk);


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
            while (!deleteList.isEmpty()) {
                int pk = deleteList.poll();
                resultMap.remove(pk);
            }

        }

        synchronized (this) {
            notifyAll();
        }


//            //变更主键处理
//            if (i < results.length - 1) {
//                LinkedHashMap<Integer, Integer> PKChangeMap = results[i + 1].getPKChangeMap();
//                for (Map.Entry<Integer, Integer> entry : PKChangeMap.entrySet()) {
//                    int oldPK = entry.getKey();
//                    int newPK = entry.getValue();
//                    if (insertMap.containsKey(oldPK)) {
//                        insertMap.put(newPK, insertMap.get(oldPK));
//                        insertMap.remove(oldPK);
//                        updateMap.put(newPK, updateMap.get(oldPK));
//                        updateMap.remove(oldPK);
//                    }
//                }
//            }
//
//            resultMap.putAll(insertMap);
////            updateMapAll.putAll(updateMap);
//            for (Map.Entry<Integer,byte[]> entry : updateMap.entrySet()){
//                updateMapAll.put(entry.getKey(),entry.getValue());
//            }
//            deleteSetAll.addAll(deleteSet);
//        }
//
//        for (int i : deleteSetAll) {
//            insertMapAll.remove(i);
//            updateMapAll.remove(i);
//        }
//
//        for (Map.Entry<Integer,byte[]> entry : updateMapAll.entrySet()){
//            int pk = entry.getKey();
//            byte[] update = entry.getValue();
//            byte[] insert = resultMap.get(pk);
//
//            for (int i = 0; i < LEN; i++) {
//                if(update[i] != 0){
//                    insert[i] = update[i];
//                }
//            }

    }

    public void showResult() {

        synchronized (this) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        List<Integer> pkList = new ArrayList<>();
        for (int l : resultMap.keySet()) {
            if (l <= lo || l >= hi) {
                continue;
            }
            pkList.add(l);
        }
        Collections.sort(pkList);

        ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(40 * 1024 * 1024);
        for (int pk : pkList) {
            byte[] record = resultMap.get(pk);
            buf.writeBytes(String.valueOf(pk).getBytes());
            for (int i = 0; i < KEY_NUM; i++) {
                buf.writeByte('\t');
                int offset = VAL_OFFSET_ARRAY[i];
                int len = VAL_LEN_ARRAY[i];
//                int len = record[offset];
//                buf.writeBytes(record, offset + 1, len);
                byte b;
                int n = 0;
                while ((n++ < len) && ((b = record[offset++]) != 0)) {
                    buf.writeByte(b);
                }
            }
            buf.writeByte('\n');
        }

        //log
//        logger.info("result 大小： " + buf.readableBytes());

        ChannelFuture future = Server.channel.writeAndFlush(buf);
        future.addListener(ChannelFutureListener.CLOSE);


    }


}
