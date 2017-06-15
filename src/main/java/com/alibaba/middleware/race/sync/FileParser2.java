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
public class FileParser2 {
    private String schema;
    private String table;
    private int lo;
    private int hi;

    private HashMap<Long, LinkedHashMap<String, String>> resultMap = new HashMap<>();



    public FileParser2(String schema, String table, int lo, int hi) {
        this.schema = schema;
        this.table = table;
        this.lo = lo;
        this.hi = hi;

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");
    }

//    private HashMap<Byte, MappedByteBuffer> mappedByteBufferHashMap = new HashMap<>(10);

    public void readPage(byte fileName) {
        try {

            FileChannel fileChannel = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r").getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
//            mappedByteBufferHashMap.put(fileName,mappedByteBuffer);

            while (mappedByteBuffer.hasRemaining()) {


                seekForSP(mappedByteBuffer, 5);
                char operation = (char) mappedByteBuffer.get();
                mappedByteBuffer.get();


                //检测库表
//                if (!ss[2].equals(schema) || !ss[3].equals(table)) {
//                    continue;
//                }

//                char operation = ss[4].charAt(0);
                Long pk;
                if (operation == 'I') {
                    seekForSP(mappedByteBuffer, 2);
                    pk = Long.valueOf(parseUnit(mappedByteBuffer));
//                    pk = Long.parseLong(ss[7]);
//                    indexMap.put(pk, new Record(fileName, position, fileLen));
                    LinkedHashMap<String, String> record = new LinkedHashMap<>();
                    parsKeyValue2(mappedByteBuffer, record);
                    resultMap.put(pk, record);
                } else if (operation == 'U') {
                    seekForSP(mappedByteBuffer, 1);
                    pk = Long.valueOf(parseUnit(mappedByteBuffer));
                    long newPK = Long.valueOf(parseUnit(mappedByteBuffer));
//                    pk = Long.parseLong(ss[6]);
                    //处理主键变更
                    if (pk != newPK) {
//                        long newPK = Long.parseLong(ss[7]);
                        resultMap.put(newPK, resultMap.get(pk));
                        resultMap.remove(pk);
                        pk = newPK;
                    }
                    parsKeyValue2(mappedByteBuffer, resultMap.get(pk));

                } else {
                    seekForSP(mappedByteBuffer, 1);
                    pk = Long.valueOf(parseUnit(mappedByteBuffer));
//                    pk = Long.parseLong(ss[6]);
                    resultMap.remove(pk);
                    seekForEN(mappedByteBuffer);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //指向下个单元开头
    private String parseUnit(MappedByteBuffer mappedByteBuffer) {
        int i = mappedByteBuffer.position();
        mappedByteBuffer.mark();
        while (mappedByteBuffer.get() != SP) {
        }
        int p = mappedByteBuffer.position();

        byte[] bytes = new byte[p - i - 1];
        mappedByteBuffer.reset();
        mappedByteBuffer.get(bytes);
        mappedByteBuffer.get();
        return new String(bytes);
    }

    private void parsKeyValue(String[] ss, HashMap<String, String> record) {
        for (int j = 8; j < ss.length - 2; j++) {
            String name = parseName(ss[j]);
            String value = ss[j + 2];
            record.put(name, value);
            j += 2;
        }
    }

    //mappedByteBuffer指向下行开头
    private void parsKeyValue2(MappedByteBuffer mappedByteBuffer, HashMap<String, String> record) {

        int p = mappedByteBuffer.position();
        mappedByteBuffer.mark();
        seekForEN(mappedByteBuffer);
        int end = mappedByteBuffer.position() - 1;
        if(end - p == 0){
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

            ArrayList<Long> pks = new ArrayList<>(resultMap.keySet());
            System.out.println(pks.size());
            Collections.sort(pks);
            Iterator<Long> it = pks.iterator();

            while (it.hasNext()) {
                long pk = it.next();
                if (pk <= lo || pk >= hi) {
                    continue;
                }

                LinkedHashMap<String, String> keyValue = resultMap.get(pk);


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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
