package com.alibaba.middleware.race.sync;

public class FilePointer {
    private int pos;
    private int len;

    public FilePointer(int pos, int len) {
        this.pos = pos;
        this.len = len;
    }

    public int getPos() {
        return pos;
    }

    public int getLen() {
        return len;
    }
}
