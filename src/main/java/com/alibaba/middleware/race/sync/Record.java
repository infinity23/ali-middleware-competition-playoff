package com.alibaba.middleware.race.sync;

import java.util.HashMap;

public class Record {
    private byte insertFileName;
    private int insertFilePosition;
    private int insertFileLen;
    private HashMap<String, UpdateRecord> update = new HashMap<>();

    public Record(byte insertFileName, int insertFilePosition, int insertFileLen) {
        this.insertFileName = insertFileName;
        this.insertFilePosition = insertFilePosition;
        this.insertFileLen = insertFileLen;
    }


    public HashMap<String, UpdateRecord> getUpdate() {
        return update;
    }

    public byte getInsertFileName() {
        return insertFileName;
    }

    public int getInsertFilePosition() {
        return insertFilePosition;
    }

    public int getInsertFileLen() {
        return insertFileLen;
    }

    //    public boolean containsUpdate(Integer hashCode){
//        return update.containsKey(hashCode);
//    }

    public void addUpdate(String name, int fileName, int filePosition, int fileLen) {
        update.put(name, new UpdateRecord((byte) fileName, filePosition, fileLen));
    }
}

class UpdateRecord {
    private byte updateFileName;
    private int updateFilePosition;
    private int updateFileLen;

    public UpdateRecord(byte updateFileName, int updateFilePosition, int updateFileLen) {
        this.updateFileName = updateFileName;
        this.updateFilePosition = updateFilePosition;
        this.updateFileLen = updateFileLen;
    }

    public byte getUpdateFileName() {
        return updateFileName;
    }

    public int getUpdateFilePosition() {
        return updateFilePosition;
    }

    public int getUpdateFileLen() {
        return updateFileLen;
    }
}