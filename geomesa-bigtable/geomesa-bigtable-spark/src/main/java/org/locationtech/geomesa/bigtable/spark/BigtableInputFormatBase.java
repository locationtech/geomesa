/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.bigtable.spark;

import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Adapted from HBase MultiTableInputFormatBase
 */
public abstract class BigtableInputFormatBase extends
    InputFormat<ImmutableBytesWritable, Result> {

  private static final Log LOG = LogFactory.getLog(BigtableInputFormatBase.class);

  /** Holds the set of scans used to define the input. */
  private List<Scan> scans;
  private TableName name;

  public void setName(TableName name) {
    this.name = name;
  }

  /** The reader scanning the table, can be a custom one. */
  private BigtableTableRecordReader tableRecordReader = null;

  /**
   * Builds a TableRecordReader. If no TableRecordReader was provided, uses the
   * default.
   *
   * @param split The split to work with.
   * @param context The current context.
   * @return The newly created record reader.
   * @throws IOException When creating the reader fails.
   * @throws InterruptedException when record reader initialization fails
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(
   *      org.apache.hadoop.mapreduce.InputSplit,
   *      org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    BigtableExtendedScanSplit tSplit = (BigtableExtendedScanSplit) split;
    LOG.info(MessageFormat.format("Input split length: {0} bytes.", tSplit.getLength()));

    if (tSplit.name == null) {
      throw new IOException("Cannot create a record reader because of a"
          + " previous error. Please look at the previous logs lines from"
          + " the task's full log for more details.");
    }
    final Connection connection = ConnectionFactory.createConnection(context.getConfiguration());
    Table table = connection.getTable(tSplit.name);

    if (this.tableRecordReader == null) {
      this.tableRecordReader = new BigtableTableRecordReader();
    }
    final BigtableTableRecordReader trr = this.tableRecordReader;

    BigtableExtendedScan sc = tSplit.scan;
    trr.setHTable(table);
    trr.setScan(sc);
    return new RecordReader<ImmutableBytesWritable, Result>() {

      @Override
      public void close() throws IOException {
        trr.close();
        connection.close();
      }

      @Override
      public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
        return trr.getCurrentKey();
      }

      @Override
      public Result getCurrentValue() throws IOException, InterruptedException {
        return trr.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return trr.getProgress();
      }

      @Override
      public void initialize(InputSplit inputsplit, TaskAttemptContext context)
          throws IOException, InterruptedException {
        trr.initialize(inputsplit, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return trr.nextKeyValue();
      }
    };
  }

  /**
   * Calculates the splits that will serve as input for the map tasks. The
   * number of splits matches the number of regions in a table.
   *
   * @param context The current job context.
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    if (scans.isEmpty()) {
      throw new IOException("No scans were provided.");
    }

    List<InputSplit> splits = new ArrayList<>();
    int count = 0;
    for (Scan s : scans) {
      BigtableExtendedScan scan = (BigtableExtendedScan) s;
      BigtableExtendedScanSplit split = new BigtableExtendedScanSplit(name, scan);
      splits.add(split);
      if (LOG.isDebugEnabled()) {
        LOG.debug("getSplits: split -> " + (count++) + " -> " + split);
      }
    }

    return splits;
  }


  /**
   * Allows subclasses to get the list of {@link Scan} objects.
   */
  protected List<Scan> getScans() {
    return this.scans;
  }

  /**
   * Allows subclasses to set the list of {@link Scan} objects.
   *
   * @param scans The list of {@link Scan} used to define the input
   */
  protected void setScans(List<Scan> scans) {
    this.scans = scans;
  }

  /**
   * Allows subclasses to set the {@link TableRecordReader}.
   *
   * @param tableRecordReader A different {@link TableRecordReader}
   *          implementation.
   */
  protected void setTableRecordReader(BigtableTableRecordReader tableRecordReader) {
    this.tableRecordReader = tableRecordReader;
  }

  public static String scanToString(BigtableExtendedScan scan) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] table = scan.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME);
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(table.length);
    dos.write(table);
    scan.getRowSet().writeTo(dos);
    dos.flush();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  public static BigtableExtendedScan stringToScan(String encoded) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(encoded)));
    int tableLength = dis.readInt();
    byte[] table = new byte[tableLength];
    dis.read(table, 0, tableLength);
    int available = dis.available();
    byte[] rowsetbytes = new byte[available];
    dis.readFully(rowsetbytes);
    RowSet rs = RowSet.parseFrom(rowsetbytes);
    BigtableExtendedScan scan = new BigtableExtendedScan();
    rs.getRowRangesList().forEach(scan::addRange);
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table);
    return scan;
  }
}
