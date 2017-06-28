package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;

import java.nio.MappedByteBuffer;
import java.util.concurrent.Callable;

import static com.alibaba.middleware.race.sync.Cons.*;
import static com.alibaba.middleware.race.sync.Constants.*;

public class Task3 implements Callable<Result> {
//    private MappedByteBuffer mappedByteBuffer;
//    HashMap<Long, HashMap<Byte, FilePointer>> updateMap = new HashMap<>();
//    HashSet<Long> deleteSet = new HashSet<>();
//    HashMap<Long, FilePointer> insertMap = new HashMap<>();
//    LinkedHashMap<Long, Long> PKChangeMap = new LinkedHashMap<>();

//    private byte[] mappedByteBuffer;
//    private int start;
//    private int len;

    //    private HashIntObjMap<byte[]> insertMap = HashIntObjMaps.newMutableMap(128 * 1024);
    private HashIntObjMap<byte[]> updateMap = HashIntObjMaps.newMutableMap(ARR_SIZE);
//    private ArrayList<Integer> updateList = new ArrayList<>(128 * 1024);
    private int[] updateArr = new int[ARR_SIZE];
    //    private LinkedHashMap<Integer, byte[]> updateMap = new LinkedHashMap<>();
//    private LinkedHashMap<Integer, Integer> PKChangeMap = new LinkedHashMap<>();
//    private ArrayList<Integer> oldPKList = new ArrayList<>(128 * 1024);
    private int[] oldPKArr = new int[ARR_SIZE];
//    private ArrayList<Integer> newPKList = new ArrayList<>(128 * 1024);
    private int[] newPKArr = new int[ARR_SIZE];

    //    private LinkedList<Integer> deleteList = new LinkedList<>();
//    private ArrayList<Integer> deleteList = new ArrayList<>(128 * 1024);
    private int[] deleteArr = new int[ARR_SIZE];


    //    private ConcurrentHashMap<Integer, byte[]> resultMap;
//    private HashMap<Integer, byte[]> resultMap;
    private HashIntObjMap<byte[]> resultMap;

    private MappedByteBuffer mappedByteBuffer;

    private int limit;

    private static int threadNum;
    private byte num;
    private byte carry;
    private int sum;



    private int updateIndex;
    private int pkChangeIndex;
    private int deleteIndex;




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

//    public Task3(ConcurrentHashMap<Integer, byte[]> resultConcurrentMap, MappedByteBuffer mappedByteBuffer, int limit) {
//        num = (byte) (threadNum % THREAD_NUM + 1);
//        carry = (byte) (threadNum / THREAD_NUM);
//        sum = num + carry * 16;
//        threadNum++;
////        this.resultMap = resultConcurrentMap;
//        this.mappedByteBuffer = mappedByteBuffer;
//        this.limit = limit;
//    }
//    public Task3(HashMap<Integer, byte[]> resultHashMap, MappedByteBuffer mappedByteBuffer, int limit) {
//        num = (byte) (threadNum % THREAD_NUM + 1);
//        carry = (byte) (threadNum / THREAD_NUM);
//        sum = num + carry * 16;
//        threadNum++;
////        this.resultMap = resultHashMap;
//        this.mappedByteBuffer = mappedByteBuffer;
//        this.limit = limit;
//    }
    public Task3(HashIntObjMap<byte[]> resultKolobokeMap, MappedByteBuffer mappedByteBuffer, int limit) {
        num = (byte) (threadNum % THREAD_NUM + 1);
        carry = (byte) (threadNum / THREAD_NUM);
        sum = num + carry * 16;
        threadNum++;
        this.resultMap = resultKolobokeMap;
        this.mappedByteBuffer = mappedByteBuffer;
        this.limit = limit;
    }


//    static long copyTime;
    @Override
    public Result call() throws Exception {
        read();
//        return new Result(insertMap,updateMap,PKChangeMap,deleteList);
//        return new Result(insertMap,updateMap,updateList,oldPKList, newPKList,deleteList);
//        return new Result(updateMap, updateList, oldPKList, newPKList, deleteList);

//        long start = System.currentTimeMillis();
        int[] updateTemp = new int[updateIndex];
        System.arraycopy(updateArr,0,updateTemp,0,updateIndex);
        int[] oldPKTemp = new int[pkChangeIndex];
        System.arraycopy(oldPKArr,0,oldPKTemp,0,pkChangeIndex);
        int[] newPKTemp = new int[pkChangeIndex];
        System.arraycopy(newPKArr,0,newPKTemp,0,pkChangeIndex);
        int[] deleteTemp = new int[deleteIndex];
        System.arraycopy(deleteArr,0,deleteTemp,0,deleteIndex);

//        long end = System.currentTimeMillis();
//
//        System.out.println("copy time: " + (end - start));

        return new Result(updateMap, updateTemp, oldPKTemp, newPKTemp, deleteTemp);
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


                //处理主键变更
                if (pk != newPK) {

//                    oldPKList.add(pk);
//                    newPKList.add(newPK);

                    oldPKArr[pkChangeIndex] = pk;
                    newPKArr[pkChangeIndex++] = newPK;


                    if (updateMap.containsKey(pk)) {
                        updateMap.put(newPK, updateMap.get(pk));
//                        updateList.add(newPK);
                        updateArr[updateIndex++] = newPK;
//                            updateMap.remove(pk);
                    }

                    pk = newPK;
                }

                if (resultMap.containsKey(pk)) {
//                    try {
                        parseUpdateKeyValue(mappedByteBuffer, resultMap.get(pk));
//                    }catch (NullPointerException e){
//
//                    }
                    continue;
                }

                byte[] update;
                if (!updateMap.containsKey(pk)) {
                    update = new byte[LEN];
                    updateMap.put(pk, update);
//                    updateList.add(pk);
                    updateArr[updateIndex++] = newPK;

                } else {
                    update = updateMap.get(pk);
                }
                parseUpdateKeyValue(mappedByteBuffer, update);

            } else {
                pk = parsePK(mappedByteBuffer);

//                if(resultMap.containsKey(pk)) {
////                    synchronized (Task3.class) {
//                        resultMap.remove(pk);
////                    }
//                }else {
////                    deleteList.add(pk);
//                    deleteArr[deleteIndex++] = pk;
//                }

//                deleteList.add(pk);
                deleteArr[deleteIndex++] = pk;
                //跳过剩余
                skipNBytes(mappedByteBuffer, SUFFIX);
                seekForEN(mappedByteBuffer);
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
