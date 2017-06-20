package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

public class Result {
    private HashMap<Long, FilePointer> insertMap;
    private HashMap<Long, HashMap<Byte, FilePointer>> updateMap;
    private HashSet<Long> deleteSet;
    private LinkedHashMap<Long, Long> PKChangeMap;

    public Result(HashMap<Long, FilePointer> insertMap, HashMap<Long, HashMap<Byte, FilePointer>> updateMap, HashSet<Long> deleteSet, LinkedHashMap<Long, Long> PKChangeMap) {
        this.insertMap = insertMap;
        this.updateMap = updateMap;
        this.deleteSet = deleteSet;
        this.PKChangeMap = PKChangeMap;
    }

    public HashMap<Long, FilePointer> getInsertMap() {
        return insertMap;
    }

    public HashMap<Long, HashMap<Byte, FilePointer>> getUpdateMap() {
        return updateMap;
    }

    public HashSet<Long> getDeleteSet() {
        return deleteSet;
    }

    public LinkedHashMap<Long, Long> getPKChangeMap() {
        return PKChangeMap;
    }
}
