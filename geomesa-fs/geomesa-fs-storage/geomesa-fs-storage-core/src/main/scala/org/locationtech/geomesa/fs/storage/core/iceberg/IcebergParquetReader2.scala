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
//class IcebergParquetReader2 {
//
//}
//
//import org.apache.iceberg.*;
//  import org.apache.iceberg.data.*;
//  import org.apache.iceberg.data.parquet.GenericParquetReaders;
//  import org.apache.iceberg.deletes.Deletes;
//  import org.apache.iceberg.deletes.PositionDeleteIndex;
//  import org.apache.iceberg.deletes.PositionDeleteIndexUtil;
//  import org.apache.iceberg.expressions.*;
//  import org.apache.iceberg.io.*;
//  import org.apache.iceberg.parquet.Parquet;
//  import org.apache.iceberg.relocated.com.google.common.collect.*;
//  import org.apache.iceberg.types.TypeUtil;
//  import org.apache.iceberg.types.Types;
//  import org.apache.iceberg.util.*;
//
//  import java.io.IOException;
//  import java.io.UncheckedIOException;
//  import java.util.*;
//  import java.util.function.Function;
//  import java.util.function.Predicate;
//
//  /**
//   * Reads Parquet data files from an Iceberg table with full delete support,
//   * without requiring ORC dependencies.
//   */
//  public class ParquetOnlyReader {
//
//      public static CloseableIterable<Record> read(Table table) {
//          return read(table.newScan());
//      }
//
//      public static CloseableIterable<Record> read(TableScan scan) {
//          Schema tableSchema = scan.table().schema();
//          Schema projection = scan.schema();
//          boolean caseSensitive = scan.isCaseSensitive();
//          FileIO io = scan.table().io();
//
//          CloseableIterable<FileScanTask> tasks = scan.planFiles();
//
//          return CloseableIterable.concat(
//              CloseableIterable.transform(tasks,
//                  task -> readTask(task, tableSchema, projection, caseSensitive, io))
//          );
//      }
//
//      private static CloseableIterable<Record> readTask(
//              FileScanTask task,
//              Schema tableSchema,
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
//          // Check if there are delete files
//          if (!task.deletes().isEmpty()) {
//              return readWithDeletes(task, tableSchema, projection, caseSensitive, io);
//          } else {
//              return readWithoutDeletes(task, projection, caseSensitive, io);
//          }
//      }
//
//      private static CloseableIterable<Record> readWithoutDeletes(
//              FileScanTask task,
//              Schema projection,
//              boolean caseSensitive,
//              FileIO io) {
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
//          if (residual != null && residual != Expressions.alwaysTrue()) {
//              builder.filter(residual);
//          }
//
//          return builder.build();
//      }
//
//      private static CloseableIterable<Record> readWithDeletes(
//              FileScanTask task,
//              Schema tableSchema,
//              Schema projection,
//              boolean caseSensitive,
//              FileIO io) {
//
//          // Create delete filter
//          ParquetDeleteFilter deleteFilter = new ParquetDeleteFilter(
//              io, task, tableSchema, projection, caseSensitive);
//
//          // Read schema may need extra columns for equality deletes
//          Schema readSchema = deleteFilter.requiredSchema();
//
//          // Read the data file
//          InputFile inputFile = io.newInputFile(task.file());
//
//          Parquet.ReadBuilder builder = Parquet.read(inputFile)
//              .project(readSchema)
//              .split(task.start(), task.length())
//              .caseSensitive(caseSensitive)
//              .createReaderFunc(fileSchema ->
//                  GenericParquetReaders.buildReader(readSchema, fileSchema));
//
//          Expression residual = task.residual();
//          if (residual != null && residual != Expressions.alwaysTrue()) {
//              builder.filter(residual);
//          }
//
//          CloseableIterable<Record> records = builder.build();
//
//          // Apply delete filter
//          records = deleteFilter.filter(records);
//
//          // Apply residual filter (in case delete columns were added)
//          if (residual != null && residual != Expressions.alwaysTrue()) {
//              InternalRecordWrapper wrapper = new InternalRecordWrapper(readSchema.asStruct());
//              Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
//              records = CloseableIterable.filter(records,
//                  record -> filter.eval(wrapper.wrap(record)));
//          }
//
//          return records;
//      }
//
//      /**
//       * Parquet-only delete filter that doesn't use FormatModelRegistry.
//       */
//      private static class ParquetDeleteFilter {
//          private final FileIO io;
//          private final FileScanTask task;
//          private final Schema tableSchema;
//          private final Schema requestedSchema;
//          private final Schema requiredSchema;
//          private final boolean caseSensitive;
//          private final Accessor<StructLike> posAccessor;
//
//          private PositionDeleteIndex positionDeletes;
//          private List<Predicate<Record>> equalityDeletePredicates;
//
//          ParquetDeleteFilter(
//                  FileIO io,
//                  FileScanTask task,
//                  Schema tableSchema,
//                  Schema requestedSchema,
//                  boolean caseSensitive) {
//              this.io = io;
//              this.task = task;
//              this.tableSchema = tableSchema;
//              this.requestedSchema = requestedSchema;
//              this.caseSensitive = caseSensitive;
//
//              // Calculate required schema (may need extra columns for equality deletes)
//              this.requiredSchema = computeRequiredSchema();
//              this.posAccessor = requiredSchema.accessorForField(
//                  MetadataColumns.ROW_POSITION.fieldId());
//          }
//
//          Schema requiredSchema() {
//              return requiredSchema;
//          }
//
//          private Schema computeRequiredSchema() {
//              Set<Integer> requiredIds = Sets.newLinkedHashSet();
//
//              // Start with requested schema
//              for (Types.NestedField field : requestedSchema.columns()) {
//                  requiredIds.add(field.fieldId());
//              }
//
//              // Add equality delete columns
//              for (DeleteFile deleteFile : task.deletes()) {
//                  if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
//                      requiredIds.addAll(deleteFile.equalityFieldIds());
//                  }
//              }
//
//              // Add position column if there are position deletes
//              boolean hasPosDeletes = task.deletes().stream()
//                  .anyMatch(df -> df.content() == FileContent.POSITION_DELETES);
//              if (hasPosDeletes) {
//                  requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
//              }
//
//              return TypeUtil.select(tableSchema, requiredIds);
//          }
//
//          CloseableIterable<Record> filter(CloseableIterable<Record> records) {
//              loadDeletes();
//              return applyDeletes(records);
//          }
//
//          private void loadDeletes() {
//              // Load position deletes
//              List<DeleteFile> posDeleteFiles = new ArrayList<>();
//              List<DeleteFile> eqDeleteFiles = new ArrayList<>();
//
//              for (DeleteFile deleteFile : task.deletes()) {
//                  // Verify it's Parquet
//                  if (deleteFile.format() != FileFormat.PARQUET) {
//                      throw new UnsupportedOperationException(
//                          "Only Parquet delete files are supported, found: "
//                          + deleteFile.format());
//                  }
//
//                  if (deleteFile.content() == FileContent.POSITION_DELETES) {
//                      posDeleteFiles.add(deleteFile);
//                  } else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
//                      eqDeleteFiles.add(deleteFile);
//                  }
//              }
//
//              // Load position deletes
//              if (!posDeleteFiles.isEmpty()) {
//                  positionDeletes = loadPositionDeletes(posDeleteFiles);
//              } else {
//                  positionDeletes = PositionDeleteIndex.empty();
//              }
//
//              // Load equality deletes
//              if (!eqDeleteFiles.isEmpty()) {
//                  equalityDeletePredicates = loadEqualityDeletes(eqDeleteFiles);
//              } else {
//                  equalityDeletePredicates = Collections.emptyList();
//              }
//          }
//
//          private PositionDeleteIndex loadPositionDeletes(List<DeleteFile> deleteFiles) {
//              Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
//              String dataFilePath = task.file().location();
//
//              List<PositionDeleteIndex> indexes = new ArrayList<>();
//
//              for (DeleteFile deleteFile : deleteFiles) {
//                  InputFile inputFile = io.newInputFile(deleteFile);
//                  Expression filter = Expressions.equal(
//                      MetadataColumns.DELETE_FILE_PATH.name(), dataFilePath);
//
//                  try (CloseableIterable<Record> deletes = Parquet.read(inputFile)
//                          .project(posDeleteSchema)
//                          .createReaderFunc(fileSchema ->
//                              GenericParquetReaders.buildReader(posDeleteSchema, fileSchema))
//                          .filter(filter)
//                          .build()) {
//
//                      PositionDeleteIndex index = Deletes.toPositionIndex(
//                          dataFilePath, deletes, deleteFile);
//                      indexes.add(index);
//
//                  } catch (IOException e) {
//                      throw new UncheckedIOException(
//                          "Failed to read position deletes from " + deleteFile.location(), e);
//                  }
//              }
//
//              return PositionDeleteIndexUtil.merge(indexes);
//          }
//
//          private List<Predicate<Record>> loadEqualityDeletes(List<DeleteFile> deleteFiles) {
//              // Group delete files by equality field IDs
//              Multimap<Set<Integer>, DeleteFile> filesByDeleteIds =
//                  Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
//
//              for (DeleteFile deleteFile : deleteFiles) {
//                  filesByDeleteIds.put(
//                      Sets.newHashSet(deleteFile.equalityFieldIds()),
//                      deleteFile);
//              }
//
//              List<Predicate<Record>> predicates = new ArrayList<>();
//
//              for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry :
//                      filesByDeleteIds.asMap().entrySet()) {
//
//                  Set<Integer> fieldIds = entry.getKey();
//                  Collection<DeleteFile> deletes = entry.getValue();
//
//                  Schema deleteSchema = TypeUtil.selectInIdOrder(requiredSchema, fieldIds);
//                  StructProjection projectRow = StructProjection.create(
//                      requiredSchema, deleteSchema);
//
//                  // Load all equality deletes into a set
//                  StructLikeSet deleteSet = StructLikeSet.create(deleteSchema.asStruct());
//                  InternalRecordWrapper wrapper = new InternalRecordWrapper(deleteSchema.asStruct());
//
//                  for (DeleteFile deleteFile : deletes) {
//                      InputFile inputFile = io.newInputFile(deleteFile);
//
//                      try (CloseableIterable<Record> deleteRecords = Parquet.read(inputFile)
//                              .project(deleteSchema)
//                              .createReaderFunc(fileSchema ->
//                                  GenericParquetReaders.buildReader(deleteSchema, fileSchema))
//                              .build()) {
//
//                          for (Record deleteRecord : deleteRecords) {
//                              deleteSet.add(wrapper.copyFor(deleteRecord));
//                          }
//
//                      } catch (IOException e) {
//                          throw new UncheckedIOException(
//                              "Failed to read equality deletes from " + deleteFile.location(), e);
//                      }
//                  }
//
//                  // Create predicate for this set of equality fields
//                  InternalRecordWrapper recordWrapper = new InternalRecordWrapper(requiredSchema.asStruct());
//                  Predicate<Record> predicate = record ->
//                      deleteSet.contains(projectRow.wrap(recordWrapper.wrap(record)));
//
//                  predicates.add(predicate);
//              }
//
//              return predicates;
//          }
//
//          private CloseableIterable<Record> applyDeletes(CloseableIterable<Record> records) {
//              // Create combined predicate
//              Predicate<Record> shouldKeep = record -> {
//                  // Check position deletes
//                  if (!positionDeletes.isEmpty()) {
//                      InternalRecordWrapper wrapper = new InternalRecordWrapper(requiredSchema.asStruct());
//                      long pos = (Long) posAccessor.get(wrapper.wrap(record));
//                      if (positionDeletes.isDeleted(pos)) {
//                          return false;
//                      }
//                  }
//
//                  // Check equality deletes
//                  for (Predicate<Record> eqDeletePredicate : equalityDeletePredicates) {
//                      if (eqDeletePredicate.test(record)) {
//                          return false;
//                      }
//                  }
//
//                  return true;
//              };
//
//              return CloseableIterable.filter(records, shouldKeep);
//          }
//      }
//  }
