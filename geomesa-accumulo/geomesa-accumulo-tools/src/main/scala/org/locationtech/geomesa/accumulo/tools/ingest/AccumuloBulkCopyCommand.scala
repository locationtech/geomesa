/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.geotools.api.data.DataStoreFinder
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloBulkCopyCommand.{AccumuloBulkCopyParams, BulkCopier}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.ParameterValidators.PositiveInteger
import org.locationtech.geomesa.tools.utils.{DistributedCopy, TerminalCallback}
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{Closeable, File, IOException}
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Copy a partitioned table out of one feature type and into an identical feature type
 */
class AccumuloBulkCopyCommand extends Command with StrictLogging {

  override val name = "bulk-copy"
  override val params = new AccumuloBulkCopyParams()

  override def execute(): Unit = WithClose(new BulkCopier(params))(_.run())
}

object AccumuloBulkCopyCommand extends LazyLogging {

  import scala.collection.JavaConverters._

  /**
   * Encapsulates the logic for the bulk copy operation
   *
   * @param params params
   */
  private class BulkCopier(params: AccumuloBulkCopyParams) extends Runnable with Closeable with StrictLogging {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConverters._

    // the cluster we are exporting from
    lazy private val from: Cluster = tryFrom.get

    private val tryFrom = Try {
      new Cluster(params.fromInstance, params.fromZookeepers, params.fromUser, Option(params.fromPassword).toRight(params.fromKeytab),
        params.fromCatalog, Option(params.fromConfigs))
    }

    // the cluster we are exporting to
    lazy private val to: Cluster = tryTo.get

    private val tryTo = Try {
      new Cluster(params.toInstance, params.toZookeepers, params.toUser, Option(params.toPassword).toRight(params.toKeytab),
        params.toCatalog, Option(params.toConfigs))
    }

    lazy private val exportPath: Path = {
      val defaultFs = FileSystem.get(to.conf)
      // makeQualified is a no-op if the path was already qualified (has a defined scheme and is not a relative path)
      new Path(params.exportPath).makeQualified(defaultFs.getUri, defaultFs.getWorkingDirectory)
    }

    lazy private val exportFs: FileSystem = exportPath.getFileSystem(to.conf)

    // sft should be shareable/the same from both datastores
    lazy private val sft: SimpleFeatureType = from.ds.getSchema(params.featureName)

    lazy private val indices = {
      val all = from.ds.manager.indices(sft)
      if (params.indices == null || params.indices.isEmpty) { all } else {
        val builder = Seq.newBuilder[GeoMesaFeatureIndex[_, _]]
        params.indices.asScala.foreach { ident =>
          val filtered = all.filter(_.identifier.contains(ident))
          if (filtered.isEmpty) {
            throw new ParameterException(
              s"Index '$ident' does not exist in the schema. Available indices: ${all.map(_.identifier).mkString(", ")}")
          }
          logger.debug(s"Mapped identifier $ident to ${filtered.map(_.identifier).mkString(", ")}")
          builder ++= filtered
        }
        builder.result.distinct
      }
    }

    // these get passed into our index method calls - for partitioned schemas, it must be a Seq[Some[_]],
    // while for non-partitioned schemas it must always be Seq(None)
    lazy private val partitions: Seq[Option[String]] = {
      if (sft.isPartitioned) {
        val builder = ListBuffer.empty[String]
        if (params.partitions != null && !params.partitions.isEmpty) {
          builder ++= params.partitions.asScala
        }
        if (params.partitionValues != null && !params.partitionValues.isEmpty) {
          val partitioning = TablePartition(from.ds, sft).get
          val sf = new ScalaSimpleFeature(sft, "")
          builder ++= params.partitionValues.asScala.map { value =>
            sf.setAttribute(sft.getDtgIndex.get, value)
            val partition = partitioning.partition(sf)
            logger.debug(s"Generated partition $partition from value $value")
            partition
          }
        }
        if (builder.isEmpty) {
          logger.debug("No partitions specified - loading all partitions from store")
          indices.foreach { index =>
            builder ++= index.getPartitions
          }
        }
        builder.result.distinct.sorted.map(Option.apply)
      } else {
        if ((params.partitions != null && !params.partitions.isEmpty) ||
          (params.partitionValues != null && !params.partitionValues.isEmpty)) {
          throw new ParameterException("--partition and/or --partition-value are not applicable for a non-partitioned schema")
        }
        Seq(None)
      }
    }


    override def run(): Unit = {
      // validate our params/setup
      if (exportPath.toUri.getScheme == "file") {
        throw new RuntimeException("Could not read defaultFS - this may be caused by a missing Hadoop core-site.xml file")
      }
      if (sft == null) {
        throw new ParameterException(s"Schema '${params.featureName}' does not exist in the source store")
      } else {
        val toSft = to.ds.getSchema(params.featureName)
        if (toSft == null) {
          throw new ParameterException(s"Schema '${params.featureName}' does not exist in the destination store")
        } else if (SimpleFeatureTypes.compare(sft, toSft) != 0) {
          throw new ParameterException(s"Schema '${params.featureName}' is not the same in the source and destination store")
        } else if (SimpleFeatureTypes.compareIndexConfigs(sft, toSft) != 0) {
          throw new ParameterException(
            s"Schema '${params.featureName}' does not have compatible indices in the source and destination store")
        }
      }

      // now execute the copy
      CachedThreadPool.executor(params.tableThreads) { executor =>
        partitions.foreach { partition =>
          indices.map { fromIndex =>
            val toIndex = to.ds.manager.index(sft, fromIndex.identifier)
            val partitionLogId = s"${partition.fold(s"index")(p => s"partition $p")} ${fromIndex.identifier}"
            val runnable: Runnable = () => {
              try {
                Command.user.info(s"Copying $partitionLogId")
                copy(fromIndex, toIndex, partition, partitionLogId)
                Command.user.info(s"Bulk copy complete for $partitionLogId")
              } catch {
                // catch Throwable so NoClassDefFound still gets logged
                case e: Throwable =>
                  Command.user.error(s"Error copying $partitionLogId: ${e.getMessage}")
                  logger.error(s"Error copying $partitionLogId", e)
              }
            }
            executor.submit(runnable)
          }
        }
      }
    }

    /**
     * Copy a single index + partition
     *
     * @param fromIndex from index
     * @param toIndex to index
     * @param partition partition name - must be Some if schema is partitioned
     * @param partitionLogId identifier for log messages
     */
    private def copy(
        fromIndex: GeoMesaFeatureIndex[_, _],
        toIndex: GeoMesaFeatureIndex[_, _],
        partition: Option[String],
        partitionLogId: String): Unit = {
      require(sft.isPartitioned == partition.isDefined) // sanity check - this should always be true due to our setup

      val fromTable = try { fromIndex.getTableName(partition) } catch {
        case NonFatal(e) => throw new RuntimeException("Could not get source table", e)
      }

      val tableExportPath = new Path(exportPath, fromTable)
      val distcpPath = new Path(tableExportPath, "distcp.txt")
      val copyToDir = new Path(tableExportPath, "files")
      val cloneTable = s"${fromTable}_bc_tmp"

      logger.debug(s"Source table $fromTable (${from.tableOps.tableIdMap().get(fromTable)})")
      logger.debug(s"Export path $tableExportPath")

      if (params.resume && from.tableOps.exists(cloneTable)) {
        logger.debug(s"Using existing cloned table $cloneTable - ensuring table is offline")
        from.tableOps.offline(cloneTable, true)
      } else {
        // clone the table as we have to take it offline in order to export it
        // note that cloning is just a metadata op as it shares the underlying data files (until they change)
        logger.debug(s"Checking for existence and deleting any existing cloned table $cloneTable")
        from.ds.adapter.deleteTable(cloneTable) // no-op if table doesn't exist
        logger.debug(s"Cloning $fromTable to $cloneTable")
        from.tableOps.clone(fromTable, cloneTable, false, Collections.emptyMap(), Collections.emptySet()) // use 2.0 method for compatibility
        logger.debug(s"Taking $cloneTable offline")
        from.tableOps.offline(cloneTable, true)
      }

      if (params.resume && exportFs.exists(distcpPath) && exportFs.getFileStatus(distcpPath).getLen > 0) {
        logger.debug(s"Using existing export results $distcpPath")
      } else {
        if (exportFs.exists(tableExportPath)) {
          logger.debug(s"Deleting existing export directory $tableExportPath")
          exportFs.delete(tableExportPath, true)
        }
        logger.debug(s"Exporting table to $tableExportPath")
        from.tableOps.exportTable(cloneTable, tableExportPath.toString)

        if (!exportFs.exists(distcpPath) || exportFs.getFileStatus(distcpPath).getLen == 0) {
          throw new RuntimeException(s"Could not read table export results at $distcpPath")
        }
      }

      // ensures the destination table exists
      logger.debug(s"Checking destination for table $fromTable")
      to.ds.adapter.createTable(toIndex, partition, Seq.empty)
      val toTable = try { toIndex.getTableName(partition) } catch {
        case NonFatal(e) => throw new RuntimeException("Could not get destination table", e)
      }
      logger.debug(s"Destination table $toTable (${to.tableOps.tableIdMap().get(toTable)})")

      // create splits, do this separately in case the table already exists
      val splits = new java.util.TreeSet(from.tableOps.listSplits(cloneTable))
      val existingSplits = to.tableOps.listSplits(toTable)
      splits.removeAll(existingSplits)
      if (!splits.isEmpty) {
        if (!existingSplits.isEmpty) {
          val warning = s"Detected split mismatch between source ($fromTable) and destination ($toTable) for $partitionLogId"
          Command.user.warn(warning)
          logger.warn(warning)
        }
        logger.debug(s"Adding splits to destination table $toTable")
        to.tableOps.addSplits(toTable, splits)
      }

      val hadCopyError = new AtomicBoolean(false)
      // read the distcp.txt file produced by the table export
      // consumer: (src, dest) => Unit
      def distCpConsumer(threads: Int)(consumer: (Path, Path) => Unit): Unit = {
        logger.debug(s"Reading $distcpPath")
        WithClose(IOUtils.lineIterator(exportFs.open(distcpPath), StandardCharsets.UTF_8)) { files =>
          CachedThreadPool.executor(threads) { executor =>
            files.asScala.foreach { file =>
              val runnable: Runnable = () => {
                val path = new Path(file)
                val copy = new Path(copyToDir, path.getName)
                try {
                  if (params.resume && exportFs.exists(copy) &&
                      path.getFileSystem(from.conf).getFileStatus(path).getLen == exportFs.getFileStatus(copy).getLen) {
                    logger.debug(s"Using existing copy of $path at $copy")
                  } else {
                    consumer(path, copy)
                  }
                } catch {
                  // catch Throwable so NoClassDefFound still gets logged
                  case e: Throwable =>
                    hadCopyError.set(true)
                    Command.user.error(s"Failed to copy $path to $copy")
                    logger.error(s"Failed to copy $path to $copy", e)
                }
              }
              executor.submit(runnable)
            }
          }
        }
      }

      if (params.distCp) {
        var inputPath = distcpPath
        if (params.resume) {
          logger.debug(s"Checking copy status of files in $distcpPath")
          inputPath = new Path(tableExportPath, "distcp-remaining.txt")
          WithClose(exportFs.create(inputPath, true)) { out =>
            distCpConsumer(1) { (path, _) =>
              logger.debug(s"Adding $path to distcp")
              out.writeUTF(s"$path\n")
            }
          }
        }
        new DistributedCopy(from.conf).copy(inputPath, copyToDir, TerminalCallback()) match {
          case JobSuccess(message, counts) =>
            Command.user.info(message)
            logger.debug(s"Distributed copy counters: ${counts.mkString("\n  ", "\n  ", "")}")

          case JobFailure(message) =>
            hadCopyError.set(true)
            Command.user.error(message)
            logger.error(message)
        }
      } else {
        distCpConsumer(params.fileThreads) { (path, copy) =>
          logger.debug(s"Copying $path to $copy")
          val fs = path.getFileSystem(from.conf)
          if (!FileUtil.copy(fs, path, exportFs, copy, false, true, to.conf)) {
            // consolidate error handling in the catch block
            throw new IOException(s"Failed to copy $path to $copy, copy returned false")
          }
        }
      }
      if (hadCopyError.get) {
        throw new RuntimeException("Error copying data files")
      }

      logger.debug(s"Loading rfiles from $copyToDir to $toTable")
      val importDir = to.tableOps.importDirectory(copyToDir.toString).to(toTable)
      try { importDir.ignoreEmptyDir(true) } catch {
        case _: NoSuchMethodError => // accumulo 2.0, ignore
      }
      try { importDir.load() } catch {
        case e: IllegalArgumentException => logger.trace("Error importing directory:", e) // should mean empty dir
      }

      // cleanup
      logger.debug(s"Deleting export path $tableExportPath")
      exportFs.delete(tableExportPath, true)
      logger.debug(s"Deleting clone table $cloneTable")
      from.tableOps.delete(cloneTable)
    }

    override def close(): Unit = {
      CloseWithLogging(tryFrom.toOption)
      CloseWithLogging(tryTo.toOption)
    }
  }

  /**
   * Holds state for a given Accumulo cluster
   *
   * @param instance instance name
   * @param zk instance zookeepers
   * @param user username
   * @param credentials credentials - either Right(password) or Left(keytab)
   * @param catalog catalog table
   * @param configs additional hadoop configuration files
   */
  private class Cluster(
      instance: String,
      zk: String,
      user: String,
      credentials: Either[String, String],
      catalog: String,
      configs: Option[java.util.List[File]]
    ) extends Closeable {

    private val params = Map(
      AccumuloDataStoreParams.InstanceNameParam.key -> instance,
      AccumuloDataStoreParams.ZookeepersParam.key -> zk,
      AccumuloDataStoreParams.UserParam.key -> user,
      AccumuloDataStoreParams.PasswordParam.key -> credentials.right.getOrElse(null),
      AccumuloDataStoreParams.KeytabPathParam.key -> credentials.left.getOrElse(null),
      AccumuloDataStoreParams.CatalogParam.key -> catalog,
    )

    val conf = new Configuration()
    configs.foreach { files =>
      files.asScala.foreach(f => conf.addResource(f.toURI.toURL))
    }

    val ds: AccumuloDataStore =
      try { DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore] } catch {
        case NonFatal(e) => throw new ParameterException("Unable to load datastore:", e)
      }

    if (ds == null) {
      throw new ParameterException(
        s"Unable to load datastore using provided values: ${params.map { case (k, v) => s"$k=$v" }.mkString(", ")}")
    }

    val tableOps: TableOperations = ds.connector.tableOperations()

    override def close(): Unit = ds.dispose()
  }

  @Parameters(commandDescription = "Bulk copy RFiles to a different cluster")
  class AccumuloBulkCopyParams extends RequiredTypeNameParam {

    @Parameter(names = Array("--from-instance"), description = "Source Accumulo instance name", required = true)
    var fromInstance: String = _
    @Parameter(names = Array("--from-zookeepers"), description = "Zookeepers for the source instance (host[:port], comma separated)", required = true)
    var fromZookeepers: String = _
    @Parameter(names = Array("--from-user"), description = "User name for the source instance", required = true)
    var fromUser: String = _
    @Parameter(names = Array("--from-keytab"), description = "Path to Kerberos keytab file for the source instance")
    var fromKeytab: String = _
    @Parameter(names = Array("--from-password"), description = "Connection password for the source instance")
    var fromPassword: String = _
    @Parameter(names = Array("--from-catalog"), description = "Catalog table containing the source feature type", required = true)
    var fromCatalog: String = _
    @Parameter(names = Array("--from-config"), description = "Additional Hadoop configuration file(s) to use for the source instance")
    var fromConfigs: java.util.List[File] = _

    @Parameter(names = Array("--to-instance"), description = "Destination Accumulo instance name", required = true)
    var toInstance: String = _
    @Parameter(names = Array("--to-zookeepers"), description = "Zookeepers for the destination instance (host[:port], comma separated)", required = true)
    var toZookeepers: String = _
    @Parameter(names = Array("--to-user"), description = "User name for the destination instance", required = true)
    var toUser: String = _
    @Parameter(names = Array("--to-keytab"), description = "Path to Kerberos keytab file for the destination instance")
    var toKeytab: String = _
    @Parameter(names = Array("--to-password"), description = "Connection password for the destination instance")
    var toPassword: String = _
    @Parameter(names = Array("--to-catalog"), description = "Catalog table containing the destination feature type", required = true)
    var toCatalog: String = _
    @Parameter(names = Array("--to-config"), description = "Additional Hadoop configuration file(s) to use for the destination instance")
    var toConfigs: java.util.List[File] = _

    @Parameter(
      names = Array("--export-path"),
      description = "HDFS path to use for source table export - the scheme and authority (e.g. bucket name) must match the destination table filesystem",
      required = true)
    var exportPath: String = _

    @Parameter(names = Array("--partition"), description = "Partition(s) to copy (if schema is partitioned)")
    var partitions: java.util.List[String] = _

    @Parameter(
      names = Array("--partition-value"),
      description = "Value(s) (e.g. dates) used to indicate partitions to copy (if schema is partitioned)")
    var partitionValues: java.util.List[String] = _

    @Parameter(names = Array("--index"), description = "Specific index(es) to copy, instead of all indices")
    var indices: java.util.List[String] = _

    @Parameter(
      names = Array("-t", "--threads"),
      description = "Number of index tables to copy concurrently",
      validateWith = Array(classOf[PositiveInteger]))
    var tableThreads: java.lang.Integer = 1

    @Parameter(
      names = Array("--file-threads"),
      description = "Number of files to copy concurrently, per table",
      validateWith = Array(classOf[PositiveInteger]))
    var fileThreads: java.lang.Integer = 2

    @Parameter(
      names = Array("--distcp"),
      description = "Use Hadoop DistCp to move files from one cluster to the other, instead of normal file copies")
    var distCp: Boolean = false

    @Parameter(
      names = Array("--resume"),
      description = "Resume a previously interrupted run from where it left off")
    var resume: Boolean = false
  }

}
