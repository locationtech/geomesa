/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.`export`

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg._
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.fs.storage.parquet.iceberg.IcebergMapper
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.`export`.FsRegisterIcebergCommand.FsRegisterIcebergParams
import org.locationtech.geomesa.tools.utils.NoopParameterSplitter
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.io.WithClose

import java.io.FileReader
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale, Properties}
import scala.util.control.NonFatal

class FsRegisterIcebergCommand extends FsDataStoreCommand with LazyLogging {

  import scala.collection.JavaConverters._

  override val params = new FsRegisterIcebergParams()

  override val name: String = "register-iceberg-files"

  override def execute(): Unit = {
    if (params.loadedPartitions.isEmpty) {
      throw new ParameterException("At least one of --partition or --partition-file must be specified")
    }
    if (params.icebergConfigFile == null && params.icebergConfig.isEmpty) {
      throw new ParameterException("At least one of --iceberg-config or --iceberg-config-file must be specified")
    }

    val icebergProps = new Properties()
    if (params.icebergConfigFile != null) {
      WithClose(new FileReader(params.icebergConfigFile, StandardCharsets.UTF_8))(icebergProps.load)
    }
    params.icebergConfig.asScala.foreach { case (k, v) => icebergProps.put(k, v) }
    if (!icebergProps.containsKey("catalog-impl")) {
      throw new ParameterException("Iceberg properties must specify a 'catalog-impl'")
    }

    val catalog = try {
      Class.forName(icebergProps.getProperty("catalog-impl")).getConstructor().newInstance().asInstanceOf[Catalog]
    } catch {
      case e: Throwable => throw new ParameterException(s"Could not instantiate catalog class '$name':", e)
    }

    catalog.initialize("geomesa", Collections.unmodifiableMap(icebergProps.asInstanceOf[java.util.Map[String, String]]))

    withDataStore(execute(_, catalog, icebergProps.asInstanceOf[java.util.Map[String, String]]))
  }

  private def execute(ds: FileSystemDataStore, catalog: Catalog, icebergProps: java.util.Map[String, String]): Unit = {
    val storage = try { ds.storage(params.featureName) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"No schema exists with type name ${params.featureName}", e)
    }
    if (storage.encoding != ParquetFileSystemStorage.Encoding) {
      throw new UnsupportedOperationException(s"Iceberg is only implemented for Parquet storage: found ${storage.encoding}")
    }

    params.loadedPartitions.foreach { p =>
      storage.metadata.schemes.foreach { s =>
        if (!p.values.exists(_.name == s.name)) {
          throw new IllegalArgumentException(
            s"Specified partition '$p' does not include all required scheme(s): " +
              storage.metadata.schemes.map(_.name).toSeq.sorted.mkString(", "))
        }
      }
    }

    val iceberg = new IcebergMapper(storage)
    logger.debug(s"Iceberg schema: ${SchemaParser.toJson(iceberg.schema)}")

    val namespace = Namespace.of(params.icebergNamespace)
    // TODO valid identifiers vary based on the catalog... this is for glue and not comprehensive
    val tableId = TableIdentifier.of(namespace, storage.metadata.sft.getTypeName.toLowerCase(Locale.US).replaceAll("[^a-z0-9]+", "_"))
    val table = if (catalog.tableExists(tableId)) {
      Command.user.info("Found existing table")
      catalog.loadTable(tableId)
      // TODO compare with existing schema and partitioning
    } else {
      Command.user.info("Creating new iceberg table")
      // file format v3 lets us use native geometries - but it's not yet supported in spark or trino
      // icebergProps.put(TableProperties.FORMAT_VERSION, "3")
      catalog.createTable(tableId, iceberg.schema, iceberg.spec, null, icebergProps)
    }
    logger.debug(s"Iceberg table: $table")

    val existingFiles = if (params.allowDuplicates) { Set.empty[String] } else {
      val builder = Set.newBuilder[String]
      // TODO we could probably limit this to just files that match our partition with a filter
      WithClose(table.newScan().planFiles()) { tasks =>
        tasks.asScala.foreach(task => builder += task.file().location())
      }
      builder.result()
    }

    val gmFiles = params.loadedPartitions.flatMap(p => storage.metadata.getFiles(p))
    Command.user.info(s"Registering ${gmFiles.size} files from ${params.loadedPartitions.size} partitions")

    var i = 0
    val files = gmFiles.flatMap { f =>
      i += 1
      val df = iceberg.toDataFile(table, f)
      logger.debug(s"File: $df")
      if (existingFiles.contains(df.location())) {
        Command.user.info(s"Skipping already registered file: ${df.location()}")
        None
      } else {
        Command.user.info(f"Registering file (${math.floor(100 * (i.toFloat / gmFiles.size)).toInt}%02d%% complete): ${df.location()}")
        Some(df)
      }
    }

    if (files.nonEmpty) {
      Command.user.info("Updating table")
      val append = table.newAppend()
      files.foreach(append.appendFile)
      append.commit()
    }
    Command.user.info("Complete")
  }
}

object FsRegisterIcebergCommand {

  @Parameters(commandDescription = "Register GeoMesa files with an Iceberg store")
  class FsRegisterIcebergParams extends FsParams with RequiredTypeNameParam with PartitionParam {

    @Parameter(
      names = Array("--iceberg-config-file"),
      description = "Name of a configuration file, in Java properties format")
    var icebergConfigFile: String = _

    @Parameter(
      names = Array("--iceberg-config"),
      description = "Configuration properties, in the form k=v",
      converter = classOf[KeyValueConverter],
      splitter = classOf[NoopParameterSplitter])
    var icebergConfig: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()

    @Parameter(names = Array("--iceberg-namespace"), description = "Iceberg namespace to use for tables", required = true)
    var icebergNamespace: String = _

    @Parameter(
      names = Array("--allow-duplicates"),
      description = "Do not check the table for existing files - this may cause duplicate results when querying if some files have already been registered")
    var allowDuplicates: Boolean = false
  }
}
