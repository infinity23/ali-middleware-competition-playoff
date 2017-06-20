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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;


//直接解析为数据集，不进行预处理
//内存会爆
public class FileParser2 {
    private String schema = SCHEMA;
    private String table = TABLE;
    private int lo = LO;
    private int hi = HI;

    private HashMap<Long, byte[]> resultMap = new HashMap<>();


    public FileParser2() {

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");
    }


    public void readPage(byte fileName) {
        try {

            FileChannel fileChannel = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r").getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            while (mappedByteBuffer.hasRemaining()) {

                char operation = parseOperation(mappedByteBuffer);

                long pk;
                if (operation == 'I') {
                    //null|
                    skipNBytes(mappedByteBuffer,5);

                    pk = parsePK(mappedByteBuffer);

                    byte[] record = new byte[LEN];
                    parseInsertKeyValue(mappedByteBuffer, record);
                    resultMap.put(pk, record);
                } else if (operation == 'U') {
                    pk = parsePK(mappedByteBuffer);
                    long newPK = parsePK(mappedByteBuffer);

                    //处理主键变更
                    if (pk != newPK) {
                        resultMap.put(newPK, resultMap.get(pk));
                        resultMap.remove(pk);
                        pk = newPK;
                    }
                    parseUpdateKeyValue(mappedByteBuffer, resultMap.get(pk));

                } else {
                    pk = parsePK(mappedByteBuffer);
                    resultMap.remove(pk);
                    seekForEN(mappedByteBuffer);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private void parseUpdateKeyValue(MappedByteBuffer mappedByteBuffer, byte[] record) {
        while (true) {
            int start = mappedByteBuffer.position();
            if (mappedByteBuffer.get() == EN) {
                break;
            }
            seekForSP(mappedByteBuffer, 1);
            int len = mappedByteBuffer.position() - 1 - start;
            seekForSP(mappedByteBuffer, 1);
            switch (len) {
                case KEY1_LEN:
                    fillArray(mappedByteBuffer, record, 0);
                    break;
                case KEY2_LEN:
                    fillArray(mappedByteBuffer, record, 1);
                    break;
                case KEY3_LEN:
                    fillArray(mappedByteBuffer, record, 2);
                    break;
                case KEY4_LEN:
                    fillArray(mappedByteBuffer, record, 3);
                    break;
                case KEY5_LEN:
                    fillArray(mappedByteBuffer, record, 4);
                    break;
            }
        }
    }

    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    //指向SP后
    private void parseInsertKeyValue(MappedByteBuffer mappedByteBuffer, byte[] record) {

//        skipNBytes(mappedByteBuffer,KEY1_LEN + 6);
//        fillArray(mappedByteBuffer,record,0);
//
//        skipNBytes(mappedByteBuffer,KEY2_LEN + 6);
//        fillArray(mappedByteBuffer,record,1);
//
//        skipNBytes(mappedByteBuffer,KEY3_LEN + 6);
//        fillArray(mappedByteBuffer,record,2);
//
//        skipNBytes(mappedByteBuffer,KEY4_LEN + 6);
//        fillArray(mappedByteBuffer,record,3);
//
//        skipNBytes(mappedByteBuffer,KEY5_LEN + 6);
//        fillArray(mappedByteBuffer,record,4);

//        比赛数据
        for (int i = 0; i < 5; i++) {
            skipNBytes(mappedByteBuffer, KEY_LEN_ARRAY[i] + 6);
            fillArray(mappedByteBuffer, record, i);
        }

//        //测试数据
//        for (int i = 0; i < 4; i++) {
//            skipNBytes(mappedByteBuffer, KEY_LEN_ARRAY[i] + 6);
//            fillArray(mappedByteBuffer, record, i);
//        }

        //跳过EN
        mappedByteBuffer.get();

    }

    //将value填入数组，指向SP后
    private void fillArray(MappedByteBuffer mappedByteBuffer, byte[] record, int val) {
        byte b;
        byte offset = VAL_OFFSET_ARRAY[val];
        //预留一个byte的长度
        int i = offset + 1;
        while ((b = mappedByteBuffer.get()) != SP) {
            record[i++] = b;
        }

        //计算长度
        record[offset] = (byte) (i - offset - 1);

    }

    private void skipNBytes(MappedByteBuffer mappedByteBuffer, int n) {
        mappedByteBuffer.position(mappedByteBuffer.position() + n);
    }


    //  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private char parseOperation(MappedByteBuffer mappedByteBuffer) {
        //跳过前缀
        seekForSP(mappedByteBuffer, 5);
        char op = (char) mappedByteBuffer.get();

        //为parsePK做准备
//        seekForSP(mappedByteBuffer, 2);

        skipNBytes(mappedByteBuffer,PK_NAME_LEN + 2);


        return op;
    }

    // NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private Long parsePK(MappedByteBuffer mappedByteBuffer) {

        int start = mappedByteBuffer.position();
        mappedByteBuffer.mark();
        seekForSP(mappedByteBuffer, 1);
        int len = mappedByteBuffer.position() - 1 - start;
        mappedByteBuffer.reset();

        byte[] bytes = new byte[len];
        mappedByteBuffer.get(bytes);
        mappedByteBuffer.get();

//      转为String
//        return Long.valueOf(new String((bytes)));

        //直接计算
        long val = 0;
        int scale = 1;
        for (int i = len - 1; i >= 0 ; i--) {
            val += scale * (bytes[i] - '0');
            scale *= 10;
        }
        return val;

    }





    //寻找第n个SP，指向SP下个元素
    private void seekForSP(MappedByteBuffer mappedByteBuffer, int n) {
        for (int i = 0; i < n; i++) {
            while (mappedByteBuffer.get() != SP) {
            }
        }
    }

    //寻找下个EN，指向下个元素
    private void seekForEN(MappedByteBuffer mappedByteBuffer) {
        while (mappedByteBuffer.get() != EN) {
        }
    }


    public void showResult() {
        Logger logger = LoggerFactory.getLogger(Server.class);


        List<Long> pkList = new ArrayList<>();
        for (Long l : resultMap.keySet()) {
            if (l <= lo || l >= hi) {
                continue;
            }
            pkList.add(l);
        }
        Collections.sort(pkList);


        ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(100 * 1024 * 1024);
        for (Long pk : pkList) {
            byte[] record = resultMap.get(pk);
            buf.writeBytes(String.valueOf(pk).getBytes());
            for (int i = 0; i < KEY_NUM; i++) {
                buf.writeByte('\t');
                int offset = VAL_OFFSET_ARRAY[i];
                int len = record[offset];
                buf.writeBytes(record, offset + 1, len);
            }
            buf.writeByte('\n');
        }

        //log
        logger.info("result 大小： " + buf.readableBytes());

        ChannelFuture future = Server.channel.writeAndFlush(buf);
        future.addListener(ChannelFutureListener.CLOSE);

    }
}
