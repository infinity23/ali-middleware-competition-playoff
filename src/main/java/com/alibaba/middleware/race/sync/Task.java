package com.alibaba.middleware.race.sync;

import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;

import static com.alibaba.middleware.race.sync.Cons.KEYMAP;
import static com.alibaba.middleware.race.sync.Constants.*;

public class Task implements Callable<Result>{
    private MappedByteBuffer mappedByteBuffer;
    HashMap<Long, HashMap<Byte, FilePointer>> updateMap = new HashMap<>();
    HashSet<Long> deleteSet = new HashSet<>();
    HashMap<Long, FilePointer> insertMap = new HashMap<>();
    LinkedHashMap<Long, Long> PKChangeMap = new LinkedHashMap<>();

    private int rowIndex;
    private int insertIndex;
    private byte page;

    public Task(MappedByteBuffer mappedByteBuffert, byte page) {
        this.page = page;
        this.mappedByteBuffer = mappedByteBuffer;
    }

    @Override
    public Result call() throws Exception {
        read();
        return new Result(insertMap,updateMap,deleteSet,PKChangeMap);
    }

    public void read() {

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

                    insertMap.put(pk, new FilePointer(insertIndex, len, page));
                    //最小化内存
                    updateMap.put(pk, new HashMap<Byte,FilePointer>(5,1));
                    insertIndex += len;

                } else if (operation == 'U') {
                    pk = parsePK(bytes, 1);
                    long newPK = parsePK(bytes, 1);

                    //处理主键变更
                    if (pk != newPK) {
                        PKChangeMap.put(pk,newPK);
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

                    parsKeyValueByIdx(bytes, updateMap.get(pk));

                } else {
                    pk = parsePK(bytes, 1);
                    if(insertMap.containsKey(pk)) {
                        insertMap.remove(pk);
                        updateMap.remove(pk);
                    }else {
                        deleteSet.add(pk);
                    }
                }

                rowIndex = 0;
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

        long val = 0;
        int scale = 1;
        for (int i = rowIndex - 2; i >= start; i--) {
            val += (bytes[i] - '0') * scale;
            scale *= 10;
        }

        return val;
    }


    // first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
    private void parsKeyValueByIdx(byte[] bytes, HashMap<Byte, FilePointer> update) {
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
//            byte[] value = copyArray(bytes, start, end);

            FilePointer pointer = new FilePointer(start,end - start,page);
            update.put(KEYMAP.get(key[4] + key[5]), pointer);
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



    //寻找下个EN，指向下个元素
    private void seekForEN(MappedByteBuffer mappedByteBuffer) {
        //保险起见，先跳70个字节
        mappedByteBuffer.position(mappedByteBuffer.position() + 70);
        while (mappedByteBuffer.get() != EN) {
        }
    }












}
