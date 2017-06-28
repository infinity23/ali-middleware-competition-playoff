package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;

public class Task4 implements Callable<Integer> {
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
//    private ArrayList<Integer> updateList = new ArrayList<>(128 * 1024);
    private LinkedList<Integer> updateList = new LinkedList<>();
    //    private LinkedHashMap<Integer, byte[]> updateMap = new LinkedHashMap<>();
//    private LinkedHashMap<Integer, Integer> PKChangeMap = new LinkedHashMap<>();
    private ArrayList<Integer> oldPKList = new ArrayList<>(128 * 1024);
    private ArrayList<Integer> newPKList = new ArrayList<>(128 * 1024);
    //    private LinkedList<Integer> deleteList = new LinkedList<>();
    private LinkedList<Integer> deleteList = new LinkedList<>();

    private ConcurrentHashMap<Integer, byte[]> resultMap;

    private Set<Integer> deleteSet;

    private MappedByteBuffer mappedByteBuffer;

    private int limit;

    private static int threadNum;
    private byte num;
    private byte carry;
    private int sum;


//    private Logger logger = LoggerFactory.getLogger(Server.class);


//    private static byte threadNum;

    //
//    public Task(byte[] mappedByteBuffer, int start, int len, ConcurrentHashMap<Integer, byte[]> resultMap) {
//        this.mappedByteBuffer = mappedByteBuffer;
//        this.start = start;
//        this.len = len;
//        this.resultMap = resultMap;
//    }
//    public Task(byte[] mappedByteBuffer, int start, int len) {
//        this.mappedByteBuffer = mappedByteBuffer;
//        this.start = start;
//        this.len = len;
//    }

    public Task4(ConcurrentHashMap<Integer, byte[]> resultMap, Set<Integer> deleteSet, MappedByteBuffer mappedByteBuffer, int limit) {
        this.deleteSet = deleteSet;
        num = (byte) (threadNum % THREAD_NUM + 1);
        carry = (byte) (threadNum / THREAD_NUM);
        sum = num + carry * 16;
        threadNum++;
        this.resultMap = resultMap;
        this.mappedByteBuffer = mappedByteBuffer;
        this.limit = limit;
    }

    @Override
    public Integer call() throws Exception {
        read();
        return sum;
    }


    public void read() {
        byte operation;
        int pk;
        byte[] record;

        while (mappedByteBuffer.position() < limit) {
            operation = parseOperation(mappedByteBuffer);

            if (operation == CHAR_I) {
                //null|
                skipNBytes(mappedByteBuffer, NULL_LEN);

                pk = parsePK(mappedByteBuffer);

                record = new byte[LEN];

                parseInsertKeyValue(mappedByteBuffer, record);

                resultMap.put(pk, record);

            } else if (operation == CHAR_U) {
                pk = parsePK(mappedByteBuffer);
                int newPK = parsePK(mappedByteBuffer);

                if(deleteSet.contains(pk) || deleteSet.contains(newPK)){
                    seekForEN(mappedByteBuffer);
                    continue;
                }

                //处理主键变更
                if (pk != newPK) {

//                    oldPKList.add(pk);
//                    newPKList.add(newPK);
//
//                    if (updateMap.containsKey(pk)) {
//                        updateMap.put(newPK, updateMap.get(pk));
//                        updateList.add(newPK);
////                            updateMap.remove(pk);
//                    }


                    if(resultMap.containsKey(pk)) {
                        synchronized (Task4.class) {
                            resultMap.put(newPK, resultMap.get(pk));
                            resultMap.remove(pk);
                            deleteSet.add(pk);
                        }
                    }

                    pk = newPK;
                }

                if (resultMap.containsKey(pk)) {
                    parseUpdateKeyValue(mappedByteBuffer, resultMap.get(pk));
                    continue;
                }

                //未insert, 缓存
                byte[] update;
                if (!updateMap.containsKey(pk)) {
                    update = new byte[LEN];
                    updateMap.put(pk, update);
                    updateList.add(pk);
                } else {
                    update = updateMap.get(pk);
                }
                parseUpdateKeyValue(mappedByteBuffer, update);

            } else {
                pk = parsePK(mappedByteBuffer);

                if(resultMap.containsKey(pk)) {
                    synchronized (Task4.class) {
                        resultMap.remove(pk);
                        deleteSet.add(pk);
                    }
                }else {
                    deleteList.add(pk);
                }

//                deleteList.add(pk);
                //跳过剩余
                skipNBytes(mappedByteBuffer, SUFFIX);
                seekForEN(mappedByteBuffer);
            }

        }

//        处理缓存
        int dp = 0;
        Iterator<Integer> it;

        it = deleteList.iterator();
        while(!deleteList.isEmpty()){
            while (it.hasNext()) {
                dp = it.next();
                if(resultMap.containsKey(dp)){
                    synchronized (Task4.class) {
                        resultMap.remove(dp);
                        it.remove();
                    }
                }
            }
            it = deleteList.iterator();
        }

        it = updateList.iterator();
        while(!updateList.isEmpty()) {
            while (it.hasNext()) {
                dp = it.next();
                if (deleteSet.contains(dp)) {
                    it.remove();
                } else if (resultMap.containsKey(dp)) {
                    byte[] rd = resultMap.get(dp);
                    byte[] ud = updateMap.get(dp);
                    copyUpdate(rd, ud);
                    it.remove();
                }
            }
            it = updateList.iterator();
        }
    }


    private void copyUpdate(byte[] record, byte[] update){
        for (int j = 0; j < KEY_NUM; j++) {
            int offset = VAL_OFFSET_ARRAY[j];
            if (update[offset] == 0) {
                continue;
            }
            int len = VAL_LEN_ARRAY[j];
            System.arraycopy(update, offset, record, offset, len);
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
            skipNBytes(mappedByteBuffer, KEY_LEN_INSERT_ARRAY[i]);
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

        int oldSum = oldNum + oldCarry * 16;

        if (oldNum != 0 && oldSum > sum) {
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
    private byte parseOperation(MappedByteBuffer mappedByteBuffer) {
        //跳过前缀(55 - 62)
        skipNBytes(mappedByteBuffer, PREFIX);
        seekForSP(mappedByteBuffer);

        byte op = mappedByteBuffer.get();

        //为parsePK做准备
//        seekForSP(mappedByteBuffer, 2);
        skipNBytes(mappedByteBuffer, PK_NAME_LEN_WITH_NULL);

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
