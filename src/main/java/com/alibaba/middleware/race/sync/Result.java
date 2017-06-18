package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

public class Result {
    private HashMap<Integer, FilePointer> insertMap;
    private HashMap<Integer,HashMap<String,String>> updateMap;
    private HashSet<Integer> deleteSet;
    private LinkedHashMap<Integer, Integer> PKChangeMap;

    public Result(HashMap<Integer, FilePointer> recordMap, HashMap<Integer, HashMap<String, String>> updateMap, HashSet<Integer> deleteSet, LinkedHashMap<Integer, Integer> PKChangeMap) {
        this.insertMap = recordMap;
        this.updateMap = updateMap;
        this.deleteSet = deleteSet;
        this.PKChangeMap = PKChangeMap;
    }

    public LinkedHashMap<Integer, Integer> getPKChangeMap() {
        return PKChangeMap;
    }

    public void setPKChangeMap(LinkedHashMap<Integer, Integer> PKChangeMap) {
        this.PKChangeMap = PKChangeMap;
    }

    public HashMap<Integer, FilePointer> getInsertMap() {
        return insertMap;
    }

    public void setInsertMap(HashMap<Integer, FilePointer> insertMap) {
        this.insertMap = insertMap;
    }

    public HashMap<Integer, HashMap<String, String>> getUpdateMap() {
        return updateMap;
    }

    public void setUpdateMap(HashMap<Integer, HashMap<String, String>> updateMap) {
        this.updateMap = updateMap;
    }

    public HashSet<Integer> getDeleteSet() {
        return deleteSet;
    }

    public void setDeleteSet(HashSet<Integer> deleteSet) {
        this.deleteSet = deleteSet;
    }
}
