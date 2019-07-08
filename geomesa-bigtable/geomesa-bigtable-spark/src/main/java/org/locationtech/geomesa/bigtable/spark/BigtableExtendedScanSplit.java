/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.spark;

import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigtableExtendedScanSplit extends InputSplit implements Writable, Comparable<BigtableExtendedScanSplit> {

    public BigtableExtendedScan scan;
    public TableName name;

    public BigtableExtendedScanSplit(TableName name, BigtableExtendedScan scan) {
        this.name = name;
        this.scan = scan;
    }

    public BigtableExtendedScanSplit() {}

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 1000;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {""};
    }

    @Override
    public int compareTo(BigtableExtendedScanSplit o) {
        // If The table name of the two splits is the same then compare start row
        // otherwise compare based on table names
        int tableNameComparison =
                name.compareTo(o.name);
        return tableNameComparison != 0 ? tableNameComparison :
                Bytes.compareTo(
                        scan.getRowSet().getRowRanges(0).getStartKeyClosed().toByteArray(),
                        o.scan.getRowSet().getRowRanges(0).getStartKeyClosed().toByteArray());

    }

    @Override
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, name.getName());
        Bytes.writeByteArray(out, BigtableInputFormatBase.scanToString(scan).getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = TableName.valueOf(Bytes.readByteArray(in));
        scan = BigtableInputFormatBase.stringToScan(Bytes.toString(Bytes.readByteArray(in)));
    }
}
