package com.alibaba.middleware.race.sync;

import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static com.alibaba.middleware.race.sync.Constants.*;

public class FileParser {
    private String schema;
    private String table ;
    private HashMap<Integer, Record> indexMap = new HashMap<>();
    private int lo;
    private int hi;


    public FileParser(String schema, String table, int lo, int hi) {
        this.schema = schema;
        this.table = table;
        this.lo = lo;
        this.hi = hi;

        System.getProperties().put("file.encoding", "UTF-8");
        System.getProperties().put("file.decoding", "UTF-8");
    }

    private HashMap<Byte, MappedByteBuffer> mappedByteBufferHashMap = new HashMap<>(10);

    public void readPage(byte fileName) {
        try {
//            FileInputStream fileInputStream = new FileInputStream(path + fileName);
//            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream,Constants.CHARSET);
//            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);


            FileChannel fileChannel = new RandomAccessFile(DATA_HOME + fileName + ".txt", "r").getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            mappedByteBufferHashMap.put(fileName,mappedByteBuffer);

            int position;
            int end;
            int fileLen;

            while (mappedByteBuffer.hasRemaining()) {
                position = mappedByteBuffer.position();
                mappedByteBuffer.mark();
                while (mappedByteBuffer.get() != Constants.EN) {}
                end = mappedByteBuffer.position();
                fileLen = end - position;
                byte[] bytes = new byte[fileLen];
                mappedByteBuffer.reset();
                mappedByteBuffer.get(bytes, 0, bytes.length);
                String s = new String(bytes);
                String[] ss = s.substring(1, s.length() - 1).split("\\|");

                //检测库表
                if (!ss[2].equals(schema) || !ss[3].equals(table)) {
                    continue;
                }

                char operation = ss[4].charAt(0);
                int pk;
                if (operation == 'I') {
                    pk = Integer.parseInt(ss[7]);
                    indexMap.put(pk, new Record(fileName, position, fileLen));
                } else if (operation == 'U') {
                    pk = Integer.parseInt(ss[6]);
                    //处理主键变更
                    if (!ss[6].equals(ss[7])) {
                        int newPK = Integer.parseInt(ss[7]);
                        indexMap.put(newPK, indexMap.get(pk));
                        indexMap.remove(pk);
                        pk = newPK;
                    }

                    for (int j = 8; j < ss.length - 2; j++) {
                        String name = parseName(ss[j]);
                        indexMap.get(pk).addUpdate(name, fileName, position, fileLen);
                        j += 2;
                    }

                } else {
                    pk = Integer.parseInt(ss[6]);
                    indexMap.remove(pk);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String parseName(String s) {
        int i = s.indexOf(":");
        return s.substring(0, i);
    }


    private String[] readDate(byte fileName, int filePoint, int fileLen) {

        MappedByteBuffer mappedByteBuffer = mappedByteBufferHashMap.get(fileName);

        mappedByteBuffer.position(filePoint);
        byte[] bytes = new byte[fileLen];
        mappedByteBuffer.get(bytes);

        String s = new String(bytes);

        return s.substring(1, s.length() - 1).split("\\|");

    }

    private int seekForSP(MappedByteBuffer mappedByteBuffer){
        while (mappedByteBuffer.get() != SP){}
        return mappedByteBuffer.position();
    }

    private int seekForEN(MappedByteBuffer mappedByteBuffer){
        while (mappedByteBuffer.get() != EN){}
        return mappedByteBuffer.position();
    }



    public void showResult() {
        try {
            FileWriter fileWriter = new FileWriter(MIDDLE_HOME + RESULT_FILE_NAME);

            StringBuilder stringBuilder = new StringBuilder();


            ArrayList<Integer> pks = new ArrayList<>(indexMap.keySet());
            Collections.sort(pks);
            Iterator<Integer> it = pks.iterator();

            while(it.hasNext()){
                int pk = it.next();
                if (pk <= lo || pk >= hi) {
                    continue;
                }
                Record record = indexMap.get(pk);
                byte fileName = record.getInsertFileName();
                int filePoint = record.getInsertFilePosition();
                int fileLen = record.getInsertFileLen();
                String[] ss = readDate(fileName, filePoint, fileLen);
                LinkedHashMap<String, String> map = new LinkedHashMap<>();
                for (int j = 8; j < ss.length - 2; j++) {
                    String name = parseName(ss[j]);
                    String value = ss[j + 2];
                    map.put(name, value);
                    j += 2;
                }

                HashMap<String, UpdateRecord> update = record.getUpdate();
                for (Map.Entry<String, UpdateRecord> entry : update.entrySet()) {
                    String key = entry.getKey();
                    UpdateRecord updateRecord = entry.getValue();
                    fileName = updateRecord.getUpdateFileName();
                    filePoint = updateRecord.getUpdateFilePosition();
                    fileLen = updateRecord.getUpdateFileLen();
                    ss = readDate(fileName, filePoint, fileLen);

                    for (int j = 8; j < ss.length - 2; j++) {
                        String name = parseName(ss[j]);
                        if(key.equals(name)){
                            String value = ss[j + 2];
                            map.put(name, value);
                            break;
                        }
                        j += 2;
                    }

                }

                stringBuilder.append(pk);
                for (Map.Entry<String, String> keyValue : map.entrySet()) {
                    stringBuilder.append('\t');
                    stringBuilder.append(keyValue.getValue());
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
