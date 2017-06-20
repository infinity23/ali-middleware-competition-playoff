package com.alibaba.middleware.race.sync;

public class FilePointer {
    private int pos;
    private int len;
    private byte page;

    public FilePointer(int pos, int len, byte page) {
        this.pos = pos;
        this.len = len;
        this.page = page;
    }

    public int getPos() {
        return pos;
    }

    public int getLen() {
        return len;
    }

    public byte getPage() {
        return page;
    }
}
