package com.alibaba.middleware.race.sync;

import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static com.alibaba.middleware.race.sync.Constants.*;


//直接解析为数据集，不进行预处理
//内存会爆
public class FileParser3 {
    private String schema;
    private String table;
    private int lo;
    private int hi;

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
    private HashMap<String, Byte> keyMap = new HashMap<String, Byte>(5) {
        {
            put("first_name",(byte) 0);
            put("last_name", (byte) 1);
            put( "sex", (byte) 2);
            put("score", (byte) 3);
            put("score2", (byte) 4);
        }
    };
    private HashMap<Byte, String> decodeKeyMap = new HashMap<Byte, String>(5) {
        {
            put((byte) 0,"first_name");
            put((byte) 1,"last_name");
            put((byte) 2,"sex");
            put((byte) 3,"score");
            put((byte) 4,"score2");
        }
    };

    //前四个单元长度（一般都是固定的）
    //变动较大，不太实用
//    private int start = 61;


    //主键名单元长度
    private int pkName = 8;


    private HashMap<Long, FilePointer> insertMap = new HashMap<>();
    private HashMap<Long, HashMap<Byte ,String>> updateMap = new HashMap<>();

    private MappedByteBuffer insertWriter;
    private int insertIndex;

    //行指针
    private int rowIndex;


    public FileParser3(String schema, String table, int lo, int hi) {
        this.schema = schema;
        this.table = table;
        this.lo = lo;
        this.hi = hi;

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");

        try {
            insertWriter = new RandomAccessFile(MIDDLE_HOME + "insert", "rw").getChannel().map(FileChannel.MapMode.READ_WRITE,0,1024 * 1024 * 1024);
        } catch (IOException e) {
            e.printStackTrace();
        }

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
                    insertWriter.put(bytes, rowIndex,len);
                    insertMap.put(pk, new FilePointer(insertIndex, len));
                    //最小化内存
                    updateMap.put(pk, new HashMap<Byte,String>(0,1));
                    insertIndex += len;

                } else if (operation == 'U') {
                    pk = parsePK(bytes, 1);
                    long newPK = parsePK(bytes, 1);

                    //处理主键变更
                    if (pk != newPK) {
                        updateMap.put(newPK, updateMap.get(pk));
                        updateMap.remove(pk);
                        insertMap.put(newPK, insertMap.get(pk));
                        insertMap.remove(pk);

                        if (rowIndex == bytes.length - 1) {
                            rowIndex = 0;
                            continue;
                        }

                        pk = newPK;
                    }

                    if (updateMap.get(pk) == null) {
                        System.out.println("error");
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
        String s = new String(bytes, start, rowIndex - 1 - start);
        return Long.parseLong(s);
    }

    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private void parsKeyValueByIdx(byte[] bytes, HashMap<Byte, String> update) {

        String s = new String(bytes, rowIndex, bytes.length - rowIndex);
        int i = 0;
        int p;
        while (true) {
            String key = parseName4(s, i);
            i = s.indexOf(SP, i + 1);
            i = s.indexOf(SP, i + 1);
            p = s.indexOf(SP, i + 1);
            String value = s.substring(i + 1, p);
            update.put(keyMap.get(key), value);
            if (p == s.length() - 2) {
                break;
            }
            i = p + 1;
        }
    }

    private void parsKeyValueByString(byte[] bytes, HashMap<String, String> update) {
        String s = new String(bytes);
        int i = 0;
        int p;
        while (true) {
            String key = parseName4(s, i);
            i = s.indexOf(SP, i + 1);
            i = s.indexOf(SP, i + 1);
            p = s.indexOf(SP, i + 1);
            String value = s.substring(i + 1, p);
            update.put(key, value);
            if (p == s.length() - 2) {
                break;
            }
            i = p + 1;
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
        while (mappedByteBuffer.get() != EN) {
        }
    }


    public void showResult() {
        try {
            FileWriter fileWriter = new FileWriter(MIDDLE_HOME + RESULT_FILE_NAME);
            StringBuilder stringBuilder = new StringBuilder();

            ArrayList<Long> pks = new ArrayList<>(insertMap.keySet());
            System.out.println(pks.size());
            Collections.sort(pks);
            Iterator<Long> it = pks.iterator();

            RandomAccessFile randomAccessFile = new RandomAccessFile(MIDDLE_HOME + "insert", "r");
            MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length());

            LinkedHashMap<String, String> keyValue = new LinkedHashMap<>(keyMap.size());
            while (it.hasNext()) {
                long pk = it.next();
                if (pk <= lo || pk >= hi) {
                    continue;
                }

                FilePointer filePointer = insertMap.get(pk);
                int p = filePointer.getPos();
                int l = filePointer.getLen();
                byte[] bytes = new byte[l];
                mappedByteBuffer.position(p);
                mappedByteBuffer.get(bytes);
                parsKeyValueByString(bytes, keyValue);

                HashMap<Byte, String> updates = updateMap.get(pk);
                for (Map.Entry<Byte, String> entry : updates.entrySet()) {
                    keyValue.put(decodeKeyMap.get(entry.getKey()), entry.getValue());
                }


                stringBuilder.append(pk);
                for (Map.Entry<String, String> entry : keyValue.entrySet()) {
                    stringBuilder.append('\t');
                    stringBuilder.append(entry.getValue());
                }
                stringBuilder.append('\n');
                fileWriter.write(stringBuilder.toString());

                //测试用
//                Logger logger = LoggerFactory.getLogger(Server.class);
//                stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
//                logger.info(stringBuilder.toString());

                stringBuilder.delete(0, stringBuilder.length());

            }
            fileWriter.close();
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
