package com.alibaba.middleware.race.sync;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;

public class Result {
//    private HashIntObjMap<byte[]> insertMap;
//    private LinkedHashMap<Integer, byte[]> updateMap;
//    private LinkedHashMap<Integer, Integer> PKChangeMap;
//    private LinkedList<Integer> deleteSet;

//    private HashIntObjMap<byte[]> insertMap = HashIntObjMaps.newMutableMap();
    private HashIntObjMap<byte[]> updateMap = HashIntObjMaps.newMutableMap();
//    private ArrayList<Integer> updateList = new ArrayList<>();
//    private ArrayList<Integer> oldPKList = new ArrayList<>();
//    private ArrayList<Integer> newPKList = new ArrayList<>();
//    private ArrayList<Integer> deleteList = new ArrayList<>();


    private int[] updateArr = new int[128 * 1024];
    private int[] oldPKArr = new int[128 * 1024];
    private int[] newPKArr = new int[128 * 1024];
    private int[] deleteArr = new int[128 * 1024];

    public HashIntObjMap<byte[]> getUpdateMap() {
        return updateMap;
    }

    public int[] getUpdateArr() {
        return updateArr;
    }

    public int[] getOldPKArr() {
        return oldPKArr;
    }

    public int[] getNewPKArr() {
        return newPKArr;
    }

    public int[] getDeleteArr() {
        return deleteArr;
    }

    public Result(HashIntObjMap<byte[]> updateMap, int[] updateArr, int[] oldPKArr, int[] newPKArr, int[] deleteArr) {
        this.updateMap = updateMap;
        this.updateArr = updateArr;
        this.oldPKArr = oldPKArr;
        this.newPKArr = newPKArr;
        this.deleteArr = deleteArr;
    }

//    public Result(HashIntObjMap<byte[]> insertMap, LinkedHashMap<Integer, byte[]> updateMap, LinkedHashMap<Integer, Integer> PKChangeMap, LinkedList<Integer> deleteSet) {
//        this.insertMap = insertMap;
//        this.updateMap = updateMap;
//        this.PKChangeMap = PKChangeMap;
//        this.deleteSet = deleteSet;
//    }


//    public Result(HashIntObjMap<byte[]> insertMap, HashIntObjMap<byte[]> updateMap, ArrayList<Integer> updateList, ArrayList<Integer> oldPKList, ArrayList<Integer> newPKList, ArrayList<Integer> deleteList) {
//        this.insertMap = insertMap;
//        this.updateMap = updateMap;
//        this.updateList = updateList;
//        this.oldPKList = oldPKList;
//        this.newPKList = newPKList;
//        this.deleteList = deleteList;
//    }
//    public Result(HashIntObjMap<byte[]> updateMap, ArrayList<Integer> updateList, ArrayList<Integer> oldPKList, ArrayList<Integer> newPKList, ArrayList<Integer> deleteList) {
//        this.updateMap = updateMap;
//        this.updateList = updateList;
//        this.oldPKList = oldPKList;
//        this.newPKList = newPKList;
//        this.deleteList = deleteList;
//    }


//    public HashIntObjMap<byte[]> getInsertMap() {
//        return insertMap;
//    }
//
//    public LinkedHashMap<Integer, byte[]> getUpdateMap() {
//        return updateMap;
//    }
//
//    public LinkedHashMap<Integer, Integer> getPKChangeMap() {
//        return PKChangeMap;
//    }
//
//    public LinkedList<Integer> getDeleteSet() {
//        return deleteSet;
//    }


//    public HashIntObjMap<byte[]> getInsertMap() {
//        return insertMap;
//    }

//    public HashIntObjMap<byte[]> getUpdateMap() {
//        return updateMap;
//    }
//
//    public ArrayList<Integer> getUpdateList() {
//        return updateList;
//    }
//
//    public ArrayList<Integer> getOldPKList() {
//        return oldPKList;
//    }
//
//    public ArrayList<Integer> getNewPKList() {
//        return newPKList;
//    }
//
//    public ArrayList<Integer> getDeleteList() {
//        return deleteList;
//    }
}
