package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;

public class Task2 implements Callable<Result>{
//    private MappedByteBuffer mappedByteBuffer;
//    HashMap<Long, HashMap<Byte, FilePointer>> updateMap = new HashMap<>();
//    HashSet<Long> deleteSet = new HashSet<>();
//    HashMap<Long, FilePointer> insertMap = new HashMap<>();
//    LinkedHashMap<Long, Long> PKChangeMap = new LinkedHashMap<>();

//    private byte[] mappedByteBuffer;
//    private int start;
//    private int len;

//    private HashIntObjMap<byte[]> insertMap = HashIntObjMaps.newMutableMap(128 * 1024);
    private HashIntObjMap<byte[]> updateMap = HashIntObjMaps.newMutableMap(128 * 1024);
    private ArrayList<Integer> updateList = new ArrayList<>(128 * 1024);
//    private LinkedHashMap<Integer, byte[]> updateMap = new LinkedHashMap<>();
//    private LinkedHashMap<Integer, Integer> PKChangeMap = new LinkedHashMap<>();
    private ArrayList<Integer> oldPKList = new ArrayList<>(128 * 1024);
    private ArrayList<Integer> newPKList = new ArrayList<>(128 * 1024);
//    private LinkedList<Integer> deleteList = new LinkedList<>();
    private ArrayList<Integer> deleteList = new ArrayList<>(128 * 1024);

    private ConcurrentHashMap<Integer, byte[]> resultMap;

    private MappedByteBuffer mappedByteBuffer;

    private int limit;


    private HashSet<Integer> deleteSet;
    private ConcurrentHashMap<Integer,Integer> PKChangeMap;
    private LinkedHashMap<Integer, byte[]> updateMapCache = new LinkedHashMap<>();
    private ArrayList<byte[]> PKChangeListCache = new ArrayList<>();
    private static int threadNum;
    private byte carry;
    private byte num;


    public Task2(ConcurrentHashMap<Integer, byte[]> resultMap, MappedByteBuffer mappedByteBuffer, int limit) {
        carry = (byte) (threadNum % 16 + 1);
        num = (byte) (threadNum / 16 + 1);

//        threadNum %= 16;
        this.resultMap = resultMap;
        this.mappedByteBuffer = mappedByteBuffer;
        this.limit = limit;
    }

    @Override
    public Result call() throws Exception {
        read();
//        return new Result(insertMap,updateMap,PKChangeMap,deleteList);
//        return new Result(insertMap,updateMap,updateList,oldPKList, newPKList,deleteList);
        return new Result(updateMap,updateList,oldPKList, newPKList,deleteList);
    }


    public void read() {

            while (mappedByteBuffer.position() < limit) {

                char operation = parseOperation(mappedByteBuffer);

                int pk;
                if (operation == 'I') {
                    //null|
                    skipNBytes(mappedByteBuffer, 5);

                    pk = parsePK(mappedByteBuffer);

                    byte[] record = new byte[LEN];

                    parseInsertKeyValue(mappedByteBuffer, record);

//                    record[0] = threadNum;

//                    insertMap.put(pk, record);
                    resultMap.put(pk, record);

                } else if (operation == 'U') {
                    pk = parsePK(mappedByteBuffer);
                    int newPK = parsePK(mappedByteBuffer);

                    //pk命中
                    if(resultMap.containsKey(pk)){
                        if(pk != newPK){
                            resultMap.put(newPK, resultMap.get(pk));
                            PKChangeMap.put(pk,newPK);
                        }

                        //pk未命中
                    }else{
                        //已删
                        if(deleteSet.contains(pk)){
                            seekForEN(mappedByteBuffer);
                            continue;
                        }

                        //PKchange未发生，入缓存
                        if(pk != newPK){

                        }

                        Integer lastPk = null;
                        Integer temp = pk;
                        while(temp != null){
                            lastPk = temp;
                            temp = PKChangeMap.get(temp);
                        }

                        //PKchange未发生，添加缓存
                        if(lastPk == pk){
                            byte[] update = new byte[LEN];
                            parseUpdateKeyValue(mappedByteBuffer,update);
                            updateMapCache.put(pk,update);
                        }else{
                            byte[] record = resultMap.get(lastPk);
                            parseUpdateKeyValue(mappedByteBuffer,record);

                        }

                    }

                } else if (operation == 'D'){
                    pk = parsePK(mappedByteBuffer);

//                    if(insertMap.containsKey(pk)) {
//                        insertMap.remove(pk);
//                        updateMap.remove(pk);
//                    }else {
//                        deleteList.add(pk);
//                    }

                    deleteList.add(pk);

                    //跳过剩余
                    skipNBytes(mappedByteBuffer, SUFFIX);
                    seekForEN(mappedByteBuffer);
                }
                else{
                    throw new RuntimeException("parse operation error.");
                }
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

    private byte[] cacheData(MappedByteBuffer mappedByteBuffer){
        return null;

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

        byte oldNum = record[offset - 2];
        byte oldCarry = record[offset - 1];

        if(oldNum * oldCarry < num * carry){
            seekForSP(mappedByteBuffer);
            return;
        }

        record[offset - 2] = num;
        record[offset - 1] = carry;

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

}
