package com.alibaba.middleware.race.sync;/*
package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.alibaba.middleware.race.sync.Constants.*;


//多线程正读
public class FileParser5 {
    private String schema;
    private String table;
    private int lo;
    private int hi;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private List<Future<Result>> futureList = new ArrayList<>();

    private final static int BLOCK_SIZE = 200 * 1024 * 1024;

    private HashMap<Long, FilePointer> insertMapAll = new HashMap<>();
    private HashMap<Long, HashMap<Byte, FilePointer>> updateMapAll = new HashMap<>();
    private HashSet<Long> deleteSetAll = new HashSet<>();


    public void readPages() {

        try {
            for (int i = 0; i < 10; i++) {
                FileChannel fileChannel = new RandomAccessFile(DATA_HOME + i + ".txt", "r").getChannel();
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                Future<Result> future = executorService.submit(new Task(mappedByteBuffer, (byte) i));
                futureList.add(future);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void mergeResult() {


        Result[] results = new Result[futureList.size()];
        try {
            for (int i = 0; i < futureList.size(); i++) {
                results[i] = futureList.get(i).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }


        for (int i = 0; i < results.length; i++) {
            HashMap<Long, FilePointer> insertMap = results[i].getInsertMap();
            HashMap<Long, HashMap<Byte, FilePointer>> updateMap = results[i].getUpdateMap();
            HashSet<Long> deleteSet = results[i].getDeleteSet();

            //变更主键处理
            if (i < results.length - 1) {
                LinkedHashMap<Long, Long> PKChangeMap = results[i + 1].getPKChangeMap();
                for (Map.Entry<Long, Long> entry : PKChangeMap.entrySet()) {
                    long oldPK = entry.getKey();
                    long newPK = entry.getValue();
                    if (insertMap.containsKey(oldPK)) {
                        insertMap.put(newPK, insertMap.get(oldPK));
                        insertMap.remove(oldPK);
                        updateMap.put(newPK, updateMap.get(oldPK));
                        updateMap.remove(oldPK);
                    }
                }
            }

            insertMapAll.putAll(insertMap);
            updateMapAll.putAll(updateMap);
            deleteSetAll.addAll(deleteSet);
        }

        for (long l : deleteSetAll) {
            if (insertMapAll.containsKey(l)) {
                insertMapAll.remove(l);
                updateMapAll.remove(l);
                deleteSetAll.remove(l);
            } else if (updateMapAll.containsKey(l)) {
                updateMapAll.remove(l);
                deleteSetAll.remove(l);
            }
        }

    }


    public void showResult() {

        try {

            LinkedHashMap<Byte, byte[]> keyValue = new LinkedHashMap<>(KEYMAP.size());

            HashMap<Integer, ByteBuf> resultMap = new HashMap<>();
            ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(100 * 1024 * 1024);

            for (Map.Entry<Long, FilePointer> entry : insertMapAll.entrySet()) {
                long pk = entry.getKey();
                if (pk <= lo || pk >= hi) {
                    continue;
                }















                rowIndex = 0;

                int p = entry.getValue();
                mappedByteBuffer.position(p);
                mappedByteBuffer.get(bytes);
                parsKeyValue(bytes, keyValue);

                HashMap<Byte, byte[]> updates = updateMap.get(pk);
                ByteBuf rowBuf = ByteBufAllocator.DEFAULT.buffer(MAX_KEYVALUE_SIZE);

                for (Map.Entry<Byte, byte[]> updateEntry : updates.entrySet()) {
                    keyValue.put(updateEntry.getKey(), updateEntry.getValue());
                }

                rowBuf.writeBytes(String.valueOf(pk).getBytes());
                for (Map.Entry<Byte, byte[]> e : keyValue.entrySet()) {
                    rowBuf.writeByte('\t');
                    rowBuf.writeBytes(e.getValue());
                }
                rowBuf.writeByte('\n');
                resultMap.put((int) pk, rowBuf);
            }


            ArrayList<Integer> pks = new ArrayList<>(resultMap.keySet());
            Collections.sort(pks);

            for (Integer pk : pks) {
                Server.channel.write(resultMap.get(pk));
                buf.writeBytes(resultMap.get(pk));
            }

            //log
            logger.info("result 大小： " + buf.readableBytes());

            ChannelFuture future = Server.channel.writeAndFlush(buf);
            future.addListener(ChannelFutureListener.CLOSE);

            buf.release();
            randomAccessFile.close();
        } catch (IOException e) {
            logger.error("showResult error", e);
        }
    }


}
*/
