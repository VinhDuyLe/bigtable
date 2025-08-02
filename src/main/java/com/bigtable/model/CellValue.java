package com.bigtable.model;

// Value structure, including tombstone for deletions
public class CellValue {
    protected byte[] value;
    boolean isTombstone;

    public CellValue(byte[] value, boolean isTombstone) {
        this.value = value;
        this.isTombstone = isTombstone;
    }

    public byte[] getValue() {
        return value;
    }

    public boolean isTombstone() {
        return isTombstone;
    }
}
