/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.geotools.api.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloBulkCopyCommand.AccumuloBulkCopyParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.ParameterValidators.PositiveInteger
import org.locationtech.geomesa.utils.io.WithClose

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

/**
 * Copy a partitioned table out of one feature type and into an identical feature type
 */
class AccumuloBulkCopyCommand extends AccumuloDataStoreCommand with StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  override val name = "bulk-copy"
  override val params = new AccumuloBulkCopyParams()

  override def execute(): Unit = {
    if (params.partitions == null) {
      params.partitions = new java.util.ArrayList[String]()
    }
    if (params.partitionValues == null) {
      params.partitionValues = new java.util.ArrayList[String]()
    }
    if (params.hadoopConfigs == null) {
      params.hadoopConfigs = new java.util.ArrayList[File]()
    }
    if (params.partitions.isEmpty && params.partitionValues.isEmpty) {
      throw new ParameterException("At least one of --partition or --partition-value is required")
    }
    withDataStore(exec)
  }

  protected def exec(ds: AccumuloDataStore): Unit = {
    val toParams = connection ++ Map(AccumuloDataStoreParams.CatalogParam.key -> params.toCatalog)
    WithClose(DataStoreFinder.getDataStore(toParams.asJava).asInstanceOf[AccumuloDataStore])(exec(ds, _))
  }

  protected def exec(fromDs: AccumuloDataStore, toDs: AccumuloDataStore): Unit = {
    val conf = new Configuration()
    params.hadoopConfigs.asScala.foreach(f => conf.addResource(f.toURI.toURL))

    val exportPath = {
      val defaultFs = FileSystem.get(conf)
      // makeQualified is a no-op if the path was already qualified (has a defined scheme and is not a relative path)
      new Path(params.exportPath).makeQualified(defaultFs.getUri, defaultFs.getWorkingDirectory)
    }
    if (exportPath.toUri.getScheme == "file") {
      throw new RuntimeException("Could not read defaultFS - this may be caused by a missing Hadoop core-site.xml file")
    }

    val exportFs = exportPath.getFileSystem(conf)
    val tableOps = fromDs.connector.tableOperations()

    // sft should be shareable/the same from both datastores
    val sft = fromDs.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist in the store")
    } else if (!sft.isPartitioned) {
      throw new ParameterException(s"Schema '${params.featureName}' is not partitioned")
    } else if (toDs.getSchema(params.featureName) == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist in the destination store")
    } else if (!toDs.getSchema(params.featureName).isPartitioned) {
      throw new ParameterException(s"Schema '${params.featureName}' is not partitioned in the destination store")
    }

    val partitions = {
      val builder = Seq.newBuilder[String]
      builder ++= params.partitions.asScala
      if (!params.partitionValues.isEmpty) {
        val partitioning = TablePartition(fromDs, sft).get
        val sf = new ScalaSimpleFeature(sft, "")
        builder ++= params.partitionValues.asScala.map { value =>
          sf.setAttribute(sft.getDtgIndex.get, value)
          val partition = partitioning.partition(sf)
          logger.debug(s"Generated partition $partition from value $value")
          partition
        }
      }
      builder.result.distinct.sorted
    }

    fromDs.manager.indices(sft).foreach { fromIndex =>
      Command.user.info(s"Copying index ${fromIndex.identifier}")
      val toIndex = toDs.manager.index(sft, fromIndex.identifier)
      partitions.foreach { partition =>
        val fromTable = fromIndex.getTableNames(Some(partition)).headOption.orNull
        if (fromTable == null) {
          Command.user.warn(s"Ignoring non-existent partition ${fromIndex.identifier} $partition")
        } else {
          val tableExportPath = new Path(exportPath, fromTable)
          Command.user.info(s"Exporting partition $partition to $tableExportPath")
          logger.debug(s"Source table $fromTable (${tableOps.tableIdMap().get(fromTable)})")
          if (exportFs.exists(tableExportPath)) {
            logger.debug(s"Deleting existing export directory $tableExportPath")
            exportFs.delete(tableExportPath, true)
          }
          // clone the table as we have to take it offline in order to export it
          // note that cloning is just a metadata op as it shares the underlying data files (until they change)
          val cloneTable = s"${fromTable}_tmp"
          logger.debug(s"Cloning $fromTable to $cloneTable")
          fromDs.adapter.deleteTable(cloneTable) // no-op if table doesn't exist
          tableOps.clone(fromTable, cloneTable, false, Collections.emptyMap(), Collections.emptySet()) // use 2.0 method for compatibility
          tableOps.offline(cloneTable, true)
          tableOps.exportTable(cloneTable, tableExportPath.toString)
          val distcpPath = new Path(tableExportPath, "distcp.txt")
          if (!exportFs.exists(distcpPath)) {
            throw new RuntimeException(s"Could not read table export results: $distcpPath")
          }

          // ensures the destination table exists
          logger.debug("Creating destination table")
          toDs.adapter.createTable(toIndex, Some(partition), Seq.empty)
          val toTable = toIndex.getTableNames(Some(partition)).headOption.getOrElse {
            throw new RuntimeException(s"Could not get destination table for index ${fromIndex.identifier} and partition $partition")
          }
          logger.debug(s"Destination table $toTable (${tableOps.tableIdMap().get(toTable)})")
          // create splits, do this separately in case the table already exists
          val splits = new java.util.TreeSet(tableOps.listSplits(cloneTable))
          val existingSplits = tableOps.listSplits(toTable)
          splits.removeAll(existingSplits)
          if (!splits.isEmpty) {
            if (!existingSplits.isEmpty) {
              logger.warn(s"Detected split mismatch between source ($fromTable) and destination ($toTable)")
            }
            logger.debug("Adding splits to destination table")
            tableOps.addSplits(toTable, splits)
          }

          logger.debug(s"Reading $distcpPath")
          WithClose(IOUtils.lineIterator(exportFs.open(distcpPath), StandardCharsets.UTF_8)) { files =>
            val copyToDir = new Path(tableExportPath, "files")
            val dirs = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())
            val executor = Executors.newFixedThreadPool(params.copyThreads)
            try {
              files.asScala.foreach { file =>
                val path = new Path(file)
                val fs = path.getFileSystem(conf)
                if (fs.getUri == exportFs.getUri) {
                  // if it's in the same FS, we can load it directly
                  dirs.add(path.getParent.toString)
                } else {
                  // otherwise we have to copy it to the destination FS
                  val copy = new Path(copyToDir, path.getName)
                  executor.submit(() => {
                    logger.debug(s"Copying $path to $copy")
                    if (FileUtil.copy(fs, path, exportFs, copy, false, false, conf)) {
                      dirs.add(copyToDir.toString)
                    } else {
                      logger.warn(s"Failed to copy $path to $copy")
                    }
                  })
                }
              }
            } finally {
              executor.shutdown()
            }
            while (!executor.isTerminated) {
              executor.awaitTermination(1, TimeUnit.MINUTES)
            }
            dirs.asScala.toSeq.sorted.foreach { dir =>
              Command.user.info(s"Loading rfiles from $dir")
              val importDir = tableOps.importDirectory(dir).to(toTable)
              try { importDir.ignoreEmptyDir(true) } catch {
                case _: NoSuchMethodError => // accumulo 2.0, ignore
              }
              try { importDir.load() } catch {
                case e: IllegalArgumentException => logger.trace("Error importing directory:", e) // should mean empty dir
              }
            }
          }

          // cleanup
          Command.user.info("Bulk copy complete - cleaning up temp files")
          logger.debug(s"Deleting table $cloneTable")
          fromDs.connector.tableOperations().delete(cloneTable)
          logger.debug(s"Deleting path $tableExportPath")
          exportFs.delete(tableExportPath, true)
        }
      }
    }
  }
}

object AccumuloBulkCopyCommand {

  @Parameters(commandDescription = "Bulk copy RFiles to a different feature type")
  class AccumuloBulkCopyParams extends AccumuloDataStoreParams with RequiredTypeNameParam {

    @Parameter(
      names = Array("--to-catalog"),
      description = "Catalog table containing the destination feature type",
      required = true)
    var toCatalog: String = _

    @Parameter(
      names = Array("--export-path"),
      description = "HDFS path to used for file export - must match destination table filesystem",
      required = true)
    var exportPath: String = _

    @Parameter(names = Array("--partition"), description = "Partition(s) to copy")
    var partitions: java.util.List[String] = _

    @Parameter(names = Array("--partition-value"), description = "Value(s) used to indicate partitions to copy (e.g. dates)")
    var partitionValues: java.util.List[String] = _

    @Parameter(names = Array("--hadoop-conf"), description = "Additional Hadoop configuration file(s) to use")
    var hadoopConfigs: java.util.List[File] = _

    @Parameter(
      names = Array("-t", "--threads"),
      description = "Number of concurrent threads to use for bulk file copies",
      validateWith = Array(classOf[PositiveInteger]))
    var copyThreads: java.lang.Integer = 1
  }
}
