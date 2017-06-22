package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.Callable;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;

public class Task implements Callable<Result>{
//    private MappedByteBuffer mappedByteBuffer;
//    HashMap<Long, HashMap<Byte, FilePointer>> updateMap = new HashMap<>();
//    HashSet<Long> deleteSet = new HashSet<>();
//    HashMap<Long, FilePointer> insertMap = new HashMap<>();
//    LinkedHashMap<Long, Long> PKChangeMap = new LinkedHashMap<>();

    private byte[] data;
    private int start;
    private int len;

    private HashIntObjMap<byte[]> insertMap = HashIntObjMaps.newMutableMap();
    private LinkedHashMap<Integer, byte[]> updateMap = new LinkedHashMap<>();
    private LinkedHashMap<Integer, Integer> PKChangeMap = new LinkedHashMap<>();
    private LinkedList<Integer> deleteList = new LinkedList<>();

//    private ConcurrentHashMap<Integer, byte[]> resultMap;

//
//    public Task(byte[] data, int start, int len, ConcurrentHashMap<Integer, byte[]> resultMap) {
//        this.data = data;
//        this.start = start;
//        this.len = len;
//        this.resultMap = resultMap;
//    }
    public Task(byte[] data, int start, int len) {
        this.data = data;
        this.start = start;
        this.len = len;
    }

    @Override
    public Result call() throws Exception {
        read();
        return new Result(insertMap,updateMap,PKChangeMap,deleteList);
    }


    private int index;
    public void read() {

            index = start;
            while (index < start + len) {

                char operation = parseOperation(data);

                int pk;
                if (operation == 'I') {
                    //null|
                    skipNBytes(data, 5);

                    pk = parsePK(data);

                    byte[] record = new byte[LEN];

                    parseInsertKeyValue(data, record);

                    insertMap.put(pk, record);
//                    resultMap.put(pk, record);

                } else if (operation == 'U') {
                    pk = parsePK(data);
                    int newPK = parsePK(data);


                    //处理主键变更
                    if (pk != newPK) {

                        PKChangeMap.put(pk,newPK);
                        if(updateMap.containsKey(pk)) {
                            updateMap.put(newPK, updateMap.get(pk));
                            updateMap.remove(pk);
                        }

                        if(insertMap.containsKey(pk)) {
                            insertMap.put(newPK, insertMap.get(pk));
                            insertMap.remove(pk);
                        }

                        pk = newPK;
                    }

                    if(insertMap.containsKey(pk)){
                        parseUpdateKeyValue(data, insertMap.get(pk));
                        continue;
                    }

                    byte[] update;
                    if(!updateMap.containsKey(pk)){
                        update = new byte[LEN];
                        updateMap.put(pk,update);
                    }else{
                        update = updateMap.get(pk);
                    }
                    parseUpdateKeyValue(data, update);

                } else {
                    pk = parsePK(data);

                    if(insertMap.containsKey(pk)) {
                        insertMap.remove(pk);
                        updateMap.remove(pk);
                    }else {
                        deleteList.add(pk);
                    }

                    deleteList.add(pk);

                    //跳过剩余
                    skipNBytes(data, SUFFIX);
                    seekForEN(data);
                }

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


}
