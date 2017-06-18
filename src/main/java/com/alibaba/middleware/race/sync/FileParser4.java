package com.alibaba.middleware.race.sync;/*
package com.alibaba.middleware.race.sync;

import java.io.FileWriter;
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


//单线程倒读
public class FileParser4 {
    private String schema;
    private String table;
    private int lo;
    private int hi;
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
    private HashMap<String, Byte> keyMap = new HashMap<String, Byte>(4) {
        {
            put("first_name",(byte) 0);
            put("last_name", (byte) 1);
            put( "sex", (byte) 2);
            put("score", (byte) 3);
            put("score2", (byte) 4);
        }
    };
    private HashMap<Byte, String> decodeKeyMap = new HashMap<Byte, String>(4) {
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
    private BlockingQueue<byte[]> writeQueue = new LinkedBlockingQueue<>(100);

    private int insertIndex;

    //行指针
    private int rowIndex;
    private boolean writeFinish;

    private int index;


    public FileParser4(String schema, String table, int lo, int hi) {
        this.schema = schema;
        this.table = table;
        this.lo = lo;
        this.hi = hi;

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");


        executorService.execute(new InsertWriter());

    }

    private HashMap<Long, HashMap<Byte,String>> resultMap = new HashMap<>();
    private HashMap<Long, Integer> PKChangeMap = new HashMap<>();
    private HashSet<Long> deleteSet = new HashSet<>();

    //  |mysql-bin.00001993819933|1497265289000|middleware5|student|U|id:1:1|4231020|4231020|first_name:2:0|孙|郑|
//  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    public void readPage(byte fileName) {
        try {

//            FileChannel fileChannel = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r").getChannel();
//            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            RandomAccessFile randomAccessFile = new RandomAccessFile(MIDDLE_HOME + fileName + ".txt", "r");
            byte[] data = new byte[(int) randomAccessFile.length()];
            randomAccessFile.write(data);

            index = data.length - 1;

            while (index > 0) {


                int end = index;
                seekForEN(data);
                int start = index;

                char operation = parseOperation(data);



                long pk;
                if (operation == 'I') {
                    pk = parsePK(data, 2);

                    if(deleteSet.contains(pk)){
                        deleteSet.remove(pk);
                        continue;
                    }

                    if(pk <= lo || pk >= hi) {
                        if (PKChangeMap.containsKey(pk)) {
                            long newPk = PKChangeMap.get(pk);
                            if(!resultMap.containsKey(newPk)){
                                resultMap.put(newPk, new HashMap<Byte, String>(5));
                            }
                            parsKeyValueByIdx(data,resultMap.get(newPk));
                        }
                    }else {


                    }

                } else if (operation == 'U') {
                    pk = parsePK(data, 1);
                    long newPK = parsePK(data, 1);

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

    private void write(byte[] bytes, int p, int len){
        byte[] bytes1 = new byte[len];
        System.arraycopy(bytes,p,bytes1,0,len);
        try {
            writeQueue.put(bytes1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void



    //  |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private char parseOperation(byte[] bytes) {
        //跳过前缀
        for (int i = 0; i < 5; i++) {
            while (bytes[index] != SP) {
                index++;
            }
            index++;
        }
        char op = (char) bytes[index];

        //为parsePK做准备
        for (int i = 0; i < 2; i++) {
            while (bytes[index] != SP) {
                index++;
            }
            index++;
        }
        return op;
    }

    // NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private Long parsePK(byte[] bytes, int n) {
        int start = 0;
        for (int i = 0; i < n; i++) {
            start = index;
            while (bytes[index] != SP) {
                index++;
            }
            index++;
        }
        String s = new String(bytes, start, index - 1 - start);
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
            if(!update.containsKey(keyMap.get(key))){
                update.put(keyMap.get(key), value);
            }
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
    private void seekForEN(byte[] data) {
        index --;
        while (data[index] != EN) {
            index --;
        }
    }


    public void showResult() {
        writeFinish = true;

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

    private class InsertWriter implements Runnable {
        private MappedByteBuffer insertWriter;
        public InsertWriter() {
            try {
                insertWriter = new RandomAccessFile(MIDDLE_HOME + "insert", "rw").getChannel().map(FileChannel.MapMode.READ_WRITE,0,1024 * 1024 * 1024);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                while(true) {
                    if(writeFinish){
                        byte[] bytes = writeQueue.poll();
                        if (bytes == null){
                            break;
                        }
                        insertWriter.put(bytes);
                    }else {
                        insertWriter.put(writeQueue.take());
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }



}
*/
