package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.alibaba.middleware.race.sync.Constants.*;

//insert写中间文件，update内存
public class FileParser3 {
    private static final int MAX_KEYVALUE_SIZE = 120;
    public static final long INSERT_SIZE = 1024 * 1024 * 1560;
    private String schema = SCHEMA;
    private String table = TABLE;
    private int lo = LO;
    private int hi = HI;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    //    //    属性约定表,用索引指代属性
//    private HashMap<String, Byte> keyMap = new HashMap<String, Byte>(4) {
//        {
//            put("first_name",(byte) 0);
//            put("last_name", (byte) 1);
//            put( "sex", (byte) 2);
//            put("score", (byte) 3);
//        }
//    };
//    private HashMap<Byte, String> decodeKeyMap = new HashMap<Byte, String>(4) {
//        {
//            put((byte) 0,"first_name");
//            put((byte) 1,"last_name");
//            put((byte) 2,"sex");
//            put((byte) 3,"score");
//        }
//    };
    //    属性约定表,用索引指代属性(赛题)
    private HashMap<Integer, Byte> keyMap = new HashMap<Integer, Byte>(4) {
        {
            //[4]+[5]

//            first_name:2:0
            put('t' + '_', (byte) 0);
//            last_name:2:0
            put('_' + 'n', (byte) 1);
//            sex:2:0
            put('2' + ':', (byte) 2);
//            score:1:0
            put('e' + ':', (byte) 3);
//            score2:1:0
            put('e' + '2', (byte) 4);
        }
    };
    private HashMap<Byte, byte[]> decodeKeyMap = new HashMap<Byte, byte[]>(4) {
        {
            put((byte) 0, "first_name".getBytes(CHARSET));
            put((byte) 1, "last_name".getBytes(CHARSET));
            put((byte) 2, "sex".getBytes(CHARSET));
            put((byte) 3, "score".getBytes(CHARSET));
            put((byte) 4, "score2".getBytes(CHARSET));
        }
    };

    //前四个单元长度（一般都是固定的）
    //变动较大，不太实用
//    private int start = 61;


    //主键名单元长度
    private int pkName = 8;

    private HashMap<Long, Integer> insertMap = new LinkedHashMap<>();
    private HashMap<Long, HashMap<Byte, byte[]>> updateMap = new HashMap<>();
    private BlockingQueue<byte[]> writeQueue = new LinkedBlockingQueue<>();

    private int insertIndex;

    //行指针
    private int rowIndex;
    private boolean writeFinish;


    public FileParser3() {

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");


        executorService.execute(new InsertWriter());

    }

    //  |mysql-bin.00001993819933|1497265289000|middleware5|student|U|id:1:1|4231020|4231020|first_name:2:0|孙|郑|
//  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    public void readPage(byte fileName) {
        try {

            FileChannel fileChannel = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r").getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());


            while (mappedByteBuffer.hasRemaining()) {

                int p = mappedByteBuffer.position();
                mappedByteBuffer.position(p);
                mappedByteBuffer.mark();
                seekForEN(mappedByteBuffer);
                int e = mappedByteBuffer.position();
                mappedByteBuffer.reset();

                byte[] bytes = new byte[e - p];
                mappedByteBuffer.get(bytes);

                char operation = parseOperation(bytes);

                Long pk;
                if (operation == 'I') {
                    pk = parsePK(bytes, 2);
                    int len = bytes.length - rowIndex;
                    write(bytes, rowIndex, len);
                    insertMap.put(pk, insertIndex);
                    //最小化内存
                    updateMap.put(pk, new HashMap<Byte, byte[]>(5, 1));
                    insertIndex += MAX_KEYVALUE_SIZE;

                } else if (operation == 'U') {
                    pk = parsePK(bytes, 1);
                    long newPK = parsePK(bytes, 1);

                    //处理主键变更
                    if (pk != newPK) {
                        updateMap.put(newPK, updateMap.get(pk));
                        updateMap.remove(pk);
                        insertMap.put(newPK, insertMap.get(pk));
                        insertMap.remove(pk);

//                        if (rowIndex == bytes.length - 1) {
//                            rowIndex = 0;
//                            continue;
//                        }

                        pk = newPK;
                    }

                    parsKeyValueByIdx(bytes, updateMap.get(pk));

                } else {
                    pk = parsePK(bytes, 1);
                    insertMap.remove(pk);
                    updateMap.remove(pk);
                }

                rowIndex = 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void write(byte[] bytes, int rowIndex, int len) {
        byte[] temp = new byte[MAX_KEYVALUE_SIZE];
        System.arraycopy(bytes, rowIndex, temp, 0, len);
        writeQueue.offer(temp);
    }


    //  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private char parseOperation(byte[] bytes) {
        //跳过前缀
        for (int i = 0; i < 5; i++) {
            while (bytes[rowIndex] != SP) {
                rowIndex++;
            }
            rowIndex++;
        }
        char op = (char) bytes[rowIndex];

        //为parsePK做准备
        for (int i = 0; i < 2; i++) {
            while (bytes[rowIndex] != SP) {
                rowIndex++;
            }
            rowIndex++;
        }
        return op;
    }

    // NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private Long parsePK(byte[] bytes, int n) {
        int start = 0;
        for (int i = 0; i < n; i++) {
            start = rowIndex;
            while (bytes[rowIndex] != SP) {
                rowIndex++;
            }
            rowIndex++;
        }

        long val = 0;
        int scale = 1;
        for (int i = rowIndex - 2; i >= start; i--) {
            val += (bytes[i] - '0') * scale;
            scale *= 10;
        }

        return val;
    }

    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
//    private void parsKeyValueByIdx(byte[] bytes, HashMap<Byte, String> update) {
//        String s = new String(bytes, rowIndex, bytes.length - rowIndex);
//        int i = 0;
//        int p;
//        while (true) {
//            String key = parseName4(s, i);
//            i = s.indexOf(SP, i + 1);
//            i = s.indexOf(SP, i + 1);
//            p = s.indexOf(SP, i + 1);
//            String value = s.substring(i + 1, p);
//            update.put(keyMap.get(key), value);
//            if (p == s.length() - 2) {
//                break;
//            }
//            i = p + 1;
//        }
//    }

    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private void parsKeyValueByIdx(byte[] bytes, HashMap<Byte, byte[]> update) {
        int start = 0;
        int end = 0;

        while (rowIndex < bytes.length - 1) {
            start = rowIndex;
            SkipSP(bytes);
            end = rowIndex - 1;
            byte[] key = copyArray(bytes, start, end);

            SkipSP(bytes);
            start = rowIndex;
            SkipSP(bytes);
            end = rowIndex - 1;
            byte[] value = copyArray(bytes, start, end);

            update.put(keyMap.get(key[4] + key[5]), value);
        }
    }

    private byte[] copyArray(byte[] bytes, int start, int end) {
        byte[] newArray = new byte[end - start];
        System.arraycopy(bytes, start, newArray, 0, newArray.length);
        return newArray;
    }

    //跳过n个SP，指向下一个元素
    private void SkipSP(byte[] bytes) {
        while (bytes[rowIndex] != SP) {
            rowIndex++;
        }
        rowIndex++;
    }

    //  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private void parsKeyValue(byte[] bytes, HashMap<Byte, byte[]> update) {
        int start = 0;
        int end = 0;

        while (bytes[rowIndex] != EN) {
            start = rowIndex;
            SkipSP(bytes);
            end = rowIndex - 1;
            byte[] key = copyArray(bytes, start, end);

            SkipSP(bytes);
            start = rowIndex;
            SkipSP(bytes);
            end = rowIndex - 1;
            byte[] value = copyArray(bytes, start, end);

            update.put(keyMap.get(key[4] + key[5]), value);
        }
    }


    //mappedByteBuffer指向下行开头
    private void parsKeyValue2(MappedByteBuffer mappedByteBuffer, HashMap<String, String> record) {

        int p = mappedByteBuffer.position();
        mappedByteBuffer.mark();
        seekForEN(mappedByteBuffer);
        int end = mappedByteBuffer.position() - 1;
        if (end - p == 0) {
            return;
        }
        mappedByteBuffer.reset();
        byte[] bytes = new byte[end - p];
        mappedByteBuffer.get(bytes);
        mappedByteBuffer.get();

        String s = new String(bytes);
        int i = 0;
        while (true) {
            String key = parseName4(s, i);
            i = s.indexOf(SP, i + 1);
            i = s.indexOf(SP, i + 1);
            p = s.indexOf(SP, i + 1);
            String value = s.substring(i + 1, p);
            record.put(key, value);
            if (p == s.length() - 1) {
                break;
            }
            i = p + 1;
        }

    }

    private String parseName4(String s, int index) {
        int i = s.indexOf(":", index + 1);
        return s.substring(index, i);
    }

    private String parseName(String s) {
        int i = s.indexOf(":");
        return s.substring(0, i);
    }

    private String parseName2(MappedByteBuffer mappedByteBuffer) {
        int p = mappedByteBuffer.position();
        mappedByteBuffer.mark();

        while (mappedByteBuffer.get() != CO) {
        }
        int end = mappedByteBuffer.position() - 1;
        mappedByteBuffer.reset();

        byte[] bytes = new byte[end - p];
        mappedByteBuffer.get(bytes);
        return new String(bytes);
    }

    private String parseName3(byte[] bytes, int i) {
        int p = i;
        while (bytes[i] != CO) {
            i++;
        }
        return new String(bytes, p, i - p);
    }


//    private String[] readDate(byte fileName, int filePoint, int fileLen) {
//
//        MappedByteBuffer mappedByteBuffer = mappedByteBufferHashMap.get(fileName);
//
//        mappedByteBuffer.position(filePoint);
//        byte[] bytes = new byte[fileLen];
//        mappedByteBuffer.get(bytes);
//
//        String s = new String(bytes);
//
//        return s.substring(1, s.length() - 1).split("\\|");
//
//    }

    //寻找第n个SP，指向SP下个元素
    private int seekForSP(MappedByteBuffer mappedByteBuffer, int n) {
        while (n > 0) {
            if (mappedByteBuffer.get() == SP) {
                n--;
            }
        }
        return mappedByteBuffer.position();
    }

    //寻找下个EN，指向下个元素
    private void seekForEN(MappedByteBuffer mappedByteBuffer) {
        //保险起见，先跳70个字节
        mappedByteBuffer.position(mappedByteBuffer.position() + 70);
        while (mappedByteBuffer.get() != EN) {
        }
    }


    public void showResult() {
        writeFinish = true;

        try {
//            FileWriter fileWriter = new FileWriter(MIDDLE_HOME + RESULT_FILE_NAME);
//            StringBuilder stringBuilder = new StringBuilder();


            RandomAccessFile randomAccessFile = new RandomAccessFile(MIDDLE_HOME + "insert", "r");
            MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length());

            LinkedHashMap<Byte, byte[]> keyValue = new LinkedHashMap<>(keyMap.size());
            byte[] bytes = new byte[MAX_KEYVALUE_SIZE];
            HashMap<Integer, ByteBuf> resultMap = new HashMap<>();
            for (Map.Entry<Long, Integer> entry : insertMap.entrySet()) {
                long pk = entry.getKey();
                if (pk <= lo || pk >= hi) {
                    continue;
                }

//                FilePointer filePointer = insertMap.get(pk);
//                int p = filePointer.getPos();
//                int l = filePointer.getLen();
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
                Server.channel.writeAndFlush(resultMap.get(pk));
            }
            Server.channel.close();

            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class InsertWriter implements Runnable {
        private MappedByteBuffer insertWriter;

        public InsertWriter() {
            try {
                insertWriter = new RandomAccessFile(MIDDLE_HOME + "insert", "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, INSERT_SIZE);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            byte[] bytes;
            try {
                while (true) {
                    if (writeFinish) {
                        bytes = writeQueue.poll();
                        if (bytes == null) {
                            break;
                        }
                        insertWriter.put(bytes);
                    } else {

                        bytes = writeQueue.take();
                        insertWriter.put(bytes);
                    }
//                    bytes = writeQueue.poll();
//                    if(bytes != null) {
//                        insertWriter.put(bytes);
//                    }
//                    if(writeFinish){
//                        break;
//                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
