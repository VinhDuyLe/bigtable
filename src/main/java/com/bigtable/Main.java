package com.bigtable;

import com.bigtable.basic.BigTable;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        BigTable table = new BigTable("./tmp/bigtable_data");

        // Insert some data
        table.put("row1", "col1", "value1".getBytes(), System.currentTimeMillis());
        table.put("row1", "col1", "value2".getBytes(), System.currentTimeMillis());

        // Retrieve data
        byte[] value = table.get("row1", "col1", System.currentTimeMillis() + 2);
        System.out.println("Retrieved: " + ((value != null)? new String(value) : "null"));

        // Delete data
        table.delete("row1", "col1", System.currentTimeMillis() + 3);
        value = table.get("row1", "col1", System.currentTimeMillis() + 4);
        System.out.println("After delete: " + (value != null? new String(value) : "null"));
        table.close();
    }
}
