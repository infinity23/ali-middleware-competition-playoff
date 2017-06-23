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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;


//FileParse2的基础上，轻量级多线程
public class FileParser6 {
    private String schema = SCHEMA;
    private String table = TABLE;
    private int lo = LO;
    private int hi = HI;
    private Logger logger = LoggerFactory.getLogger(Server.class);


//        private HashMap<Integer, byte[]> resultMap = new HashMap<>();
//    private KMap<Long, byte[]> resultMap = KMap.withExpectedSize();
    private HashIntObjMap<byte[]> resultMap = HashIntObjMaps.newMutableMap(5000000);


    public FileParser6() {

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");
    }


    private int index = 0;
    public void readPage(byte fileName) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r");

//            FileChannel fileChannel = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r").getChannel();
//            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//
            byte[] data = new byte[(int) randomAccessFile.length()];
            randomAccessFile.read(data);
            index = 0;

            while (index < data.length) {

                char operation = parseOperation(data);

                int pk;
                if (operation == 'I') {
                    //null|
                    skipNBytes(data, 5);

                    pk = parsePK(data);

                    byte[] record = new byte[LEN];
                    parseInsertKeyValue(data, record);
                    resultMap.put(pk, record);
                } else if (operation == 'U') {
                    pk = parsePK(data);
                    int newPK = parsePK(data);

                    //处理主键变更
                    if (pk != newPK) {
                        resultMap.put(newPK, resultMap.get(pk));
                        resultMap.remove(pk);
                        pk = newPK;
                    }
                    parseUpdateKeyValue(data, resultMap.get(pk));

                } else {
                    pk = parsePK(data);
                    resultMap.remove(pk);

                    //跳过剩余
                    skipNBytes(data, SUFFIX);
                    seekForEN(data);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private void parseUpdateKeyValue(byte[] data, byte[] record) {
        while (true) {
            int start = index;
            if (data[index++] == EN) {
                break;
            }
            seekForSP(data);
            int len = index - 1 - start;
            seekForSP(data);
            switch (len) {
                case KEY1_LEN:
                    fillArray(data, record, 0);
                    break;
                case KEY2_LEN:
                    fillArray(data, record, 1);
                    break;
                case KEY3_LEN:
                    fillArray(data, record, 2);
                    break;
                case KEY4_LEN:
                    fillArray(data, record, 3);
                    break;
                case KEY5_LEN:
                    fillArray(data, record, 4);
                    break;
            }
        }
    }

    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    //指向SP后
    private void parseInsertKeyValue(byte[] data, byte[] record) {

        for (int i = 0; i < KEY_NUM; i++) {
            skipNBytes(data, KEY_LEN_ARRAY[i] + 6);
            fillArrayInsert(data, record, i);
        }

        //跳过EN
        index++;
    }

    //将value填入数组，指向SP后
    private void fillArrayInsert(byte[] data, byte[] record, int val) {
//        byte b;
//        byte offset = VAL_OFFSET_ARRAY[val];
//        //预留一个byte的长度
//        int i = offset + 1;
//        while ((b = mappedByteBuffer.get()) != SP) {
//            record[i++] = b;
//        }
//
//        //计算长度
//        record[offset] = (byte) (i - offset - 1);

        byte b;

        int i = VAL_OFFSET_ARRAY[val];
        while ((b = data[index++]) != SP) {
            record[i++] = b;
        }

    }

    private void fillArray(byte[] data, byte[] record, int val) {
//        byte b;
//        byte offset = VAL_OFFSET_ARRAY[val];
//        //预留一个byte的长度
//        int i = offset + 1;
//        while ((b = mappedByteBuffer.get()) != SP) {
//            record[i++] = b;
//        }
//
//        //计算长度
//        record[offset] = (byte) (i - offset - 1);


        //不记len清零法
        byte b;
        byte offset = VAL_OFFSET_ARRAY[val];
        byte len = VAL_LEN_ARRAY[val];

        int i = offset;
        while ((b = data[index++]) != SP) {
            record[i++] = b;
        }

        while (i < offset + len) {
            record[i++] = 0;
        }


    }

    private void skipNBytes(byte[] data, int n) {
        index += n;
    }


    // NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private int parsePK(byte[] data) {

//      转为String
//        return Long.valueOf(new String((bytes)));

        //直接计算
        int val = 0;
        byte b;
        while ((b = data[index++]) != SP) {
            val = val * 10 + b - CHAR_ZERO;
        }

        return val;
    }

    //  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private char parseOperation(byte[] data) {
        //跳过前缀(55 - 62)
//        seekForSP(mappedByteBuffer, 5);
        skipNBytes(data, 54);
        seekForSP(data);

        char op = (char) data[index++];

        //为parsePK做准备
//        seekForSP(mappedByteBuffer, 2);
        skipNBytes(data, PK_NAME_LEN + 2);

        return op;
    }


    private void seekForSP(byte[] data) {
        while (data[index++] != SP) {
        }
    }

    //寻找下个EN，指向下个元素
    private void seekForEN(byte[] data) {
        while (data[index++] != EN) {
        }
    }



    public void showResult() {


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
