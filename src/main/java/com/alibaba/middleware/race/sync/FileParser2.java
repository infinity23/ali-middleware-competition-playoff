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
import java.util.List;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;


//直接解析为数据集，不进行预处理
public class FileParser2 {
    private String schema = SCHEMA;
    private String table = TABLE;
    private int lo = LO;
    private int hi = HI;
    private Logger logger = LoggerFactory.getLogger(Server.class);


    //        private HashMap<Integer, byte[]> resultMap = new HashMap<>();
//    private KMap<Long, byte[]> resultMap = KMap.withExpectedSize();
    private HashIntObjMap<byte[]> resultMap = HashIntObjMaps.newMutableMap(5000000);
    private char lastOperation;
    private boolean pkChangeStart;
    private boolean pkFlag;


    public FileParser2() {

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");
    }

    int pk;
    int newPK;

    public void readPage(byte fileName) {
        try {

            FileChannel fileChannel = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r").getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            while (mappedByteBuffer.hasRemaining()) {

                int position = mappedByteBuffer.position();

                char operation = parseOperation(mappedByteBuffer);

                pkFlag = false;


                if (operation == 'I') {
                    //null|
                    skipNBytes(mappedByteBuffer, 5);

                    pk = parsePK(mappedByteBuffer);

                    byte[] record = new byte[LEN];
                    parseInsertKeyValue(mappedByteBuffer, record);
                    resultMap.put(pk, record);
                } else if (operation == 'U') {
                    pk = parsePK(mappedByteBuffer);
                    newPK = parsePK(mappedByteBuffer);

                    //处理主键变更
                    if (pk != newPK) {
                        pkFlag = true;
                        if (!pkChangeStart) {
                            logger.info("PKChange start: " + lastOperation + " to " + operation + " position: " + position + " fileName "+fileName);
                            pkChangeStart = true;
                        }

                        resultMap.put(newPK, resultMap.get(pk));
                        resultMap.remove(pk);
                        pk = newPK;
                    }
                    parseUpdateKeyValue(mappedByteBuffer, resultMap.get(pk));

                } else {
                    pk = parsePK(mappedByteBuffer);
                    resultMap.remove(pk);

                    //跳过剩余
                    skipNBytes(mappedByteBuffer, SUFFIX);
                    seekForEN(mappedByteBuffer);
                }

                if(pkChangeStart && !pkFlag){
                    logger.info("PKChange end: " + lastOperation + " to " + operation + " position: " + position + " fileName "+fileName);
                    pkChangeStart = false;
                }


                lastOperation = operation;


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
            seekForSP(mappedByteBuffer);
            int len = mappedByteBuffer.position() - 1 - start;
            seekForSP(mappedByteBuffer);
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

        for (int i = 0; i < KEY_NUM; i++) {
            skipNBytes(mappedByteBuffer, KEY_LEN_ARRAY[i] + 6);
            fillArrayInsert(mappedByteBuffer, record, i);
        }

        //跳过EN
        mappedByteBuffer.get();
    }

    //将value填入数组，指向SP后
    private void fillArrayInsert(MappedByteBuffer mappedByteBuffer, byte[] record, int val) {
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
        while ((b = mappedByteBuffer.get()) != SP) {
            record[i++] = b;
        }

    }

    private void fillArray(MappedByteBuffer mappedByteBuffer, byte[] record, int val) {
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
        while ((b = mappedByteBuffer.get()) != SP) {
            record[i++] = b;
        }

        while (i < offset + len) {
            record[i++] = 0;
        }


    }

    private void skipNBytes(MappedByteBuffer mappedByteBuffer, int n) {
        mappedByteBuffer.position(mappedByteBuffer.position() + n);
    }


    // NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private int parsePK(MappedByteBuffer mappedByteBuffer) {

//      转为String
//        return Long.valueOf(new String((bytes)));

        //直接计算
        int val = 0;
        byte b;
        while ((b = mappedByteBuffer.get()) != SP) {
            val = val * 10 + b - CHAR_ZERO;
        }

        return val;
    }

    //  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private char parseOperation(MappedByteBuffer mappedByteBuffer) {
        //跳过前缀(55 - 62)
//        seekForSP(mappedByteBuffer, 5);
        skipNBytes(mappedByteBuffer, 54);
        seekForSP(mappedByteBuffer);

        char op = (char) mappedByteBuffer.get();

        //为parsePK做准备
//        seekForSP(mappedByteBuffer, 2);
        skipNBytes(mappedByteBuffer, PK_NAME_LEN + 2);

        return op;
    }


    private void seekForSP(MappedByteBuffer mappedByteBuffer) {
        while (mappedByteBuffer.get() != SP) {
        }
    }

    //寻找下个EN，指向下个元素
    private void seekForEN(MappedByteBuffer mappedByteBuffer) {
        while (mappedByteBuffer.get() != EN) {
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

//        ChannelFuture future = Server.channel.writeAndFlush(buf);
//        future.addListener(ChannelFutureListener.CLOSE);


    }
}
