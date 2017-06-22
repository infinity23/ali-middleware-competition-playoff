package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;

import java.util.LinkedHashMap;
import java.util.LinkedList;

public class Result {
    private HashIntObjMap<byte[]> insertMap;
    private LinkedHashMap<Integer, byte[]> updateMap;
    private LinkedHashMap<Integer, Integer> PKChangeMap;
    private LinkedList<Integer> deleteSet;

    public Result(HashIntObjMap<byte[]> insertMap, LinkedHashMap<Integer, byte[]> updateMap, LinkedHashMap<Integer, Integer> PKChangeMap, LinkedList<Integer> deleteSet) {
        this.insertMap = insertMap;
        this.updateMap = updateMap;
        this.PKChangeMap = PKChangeMap;
        this.deleteSet = deleteSet;
    }

    public HashIntObjMap<byte[]> getInsertMap() {
        return insertMap;
    }

    public LinkedHashMap<Integer, byte[]> getUpdateMap() {
        return updateMap;
    }

    public LinkedHashMap<Integer, Integer> getPKChangeMap() {
        return PKChangeMap;
    }

    public LinkedList<Integer> getDeleteSet() {
        return deleteSet;
    }
}
