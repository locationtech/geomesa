/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

///***********************************************************************
// * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
// * All rights reserved. This program and the accompanying materials
// * are made available under the terms of the Apache License, Version 2.0
// * which accompanies this distribution and is available at
// * https://www.apache.org/licenses/LICENSE-2.0
// ***********************************************************************/
//
//package org.locationtech.geomesa.fs.storage.core.iceberg
//
//class IcebergParquetReader {
//
//}
//
//
//  import org.apache.iceberg.*;
//  import org.apache.iceberg.data.Record;
//  import org.apache.iceberg.data.parquet.GenericParquetReaders;
//  import org.apache.iceberg.expressions.Expression;
//  import org.apache.iceberg.expressions.Expressions;
//  import org.apache.iceberg.io.CloseableIterable;
//  import org.apache.iceberg.io.InputFile;
//  import org.apache.iceberg.parquet.Parquet;
//
//  import java.util.Map;
//
//  public class ParquetOnlyReader {
//
//      /**
//       * Read all records from a Parquet-only Iceberg table.
//       * WARNING: Does not handle delete files!
//       */
//      public static CloseableIterable<Record> read(Table table) {
//          return read(table.newScan());
//      }
//
//      /**
//       * Read records using a configured table scan.
//       */
//      public static CloseableIterable<Record> read(TableScan scan) {
//          Schema projection = scan.schema();
//          boolean caseSensitive = scan.isCaseSensitive();
//          FileIO io = scan.table().io();
//
//          CloseableIterable<FileScanTask> tasks = scan.planFiles();
//
//          return CloseableIterable.concat(
//              CloseableIterable.transform(tasks,
//                  task -> readTask(task, projection, caseSensitive, io))
//          );
//      }
//
//      private static CloseableIterable<Record> readTask(
//              FileScanTask task,
//              Schema projection,
//              boolean caseSensitive,
//              FileIO io) {
//
//          // Verify this is a Parquet file
//          if (task.file().format() != FileFormat.PARQUET) {
//              throw new UnsupportedOperationException(
//                  "Only Parquet files are supported, found: " + task.file().format());
//          }
//
//          InputFile inputFile = io.newInputFile(task.file());
//          Expression residual = task.residual();
//
//          Parquet.ReadBuilder builder = Parquet.read(inputFile)
//              .project(projection)
//              .split(task.start(), task.length())
//              .caseSensitive(caseSensitive)
//              .createReaderFunc(fileSchema ->
//                  GenericParquetReaders.buildReader(projection, fileSchema));
//
//          // Apply residual filter if present
//          if (residual != null && residual != Expressions.alwaysTrue()) {
//              builder.filter(residual);
//          }
//
//          return builder.build();
//      }
//  }
