/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import com.typesafe.scalalogging.StrictLogging
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.tools.DistCp
import org.geotools.api.data.DataStoreFinder
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.util.SchemaCopier._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.hadoop.DistributedCopyOptions
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{Closeable, File, IOException}
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.{Callable, ConcurrentHashMap}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Copies a schema from one cluster (or catalog) to another, using bulk file operations
 *
 * @param fromCluster source cluster
 * @param toCluster destination cluster
 * @param typeName name of the feature type to copy
 * @param exportDir hdfs or s3a path to use for source table export - the scheme and authority (e.g. bucket name) must match
 *                   the destination table filesystem
 * @param indices specific indices to copy, or empty to copy all indices
 * @param partitions if schema is partitioned - specific partitions to copy, or empty to copy all partitions
 * @param options other copy options
 */
class SchemaCopier(
    fromCluster: ClusterConfig,
    toCluster: ClusterConfig,
    typeName: String,
    exportDir: String,
    indices: Seq[String],
    partitions: Seq[PartitionId],
    options: CopyOptions,
  ) extends Callable[Set[CopyResult]] with Closeable with StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private val tryFrom = Try(Cluster(fromCluster))
  private val tryTo = Try(Cluster(toCluster))
  private var closed = false

  // note: all other class variables are lazy, so that we can instantiate an instance and then clean up connections on close()

  // the cluster we are exporting from
  lazy private val from: Cluster = tryFrom.get
  // the cluster we are exporting to
  lazy private val to: Cluster = tryTo.get

  lazy private val exportPath: Path = {
    val defaultFs = FileSystem.get(to.conf)
    // makeQualified is a no-op if the path was already qualified (has a defined scheme and is not a relative path)
    val path = new Path(exportDir).makeQualified(defaultFs.getUri, defaultFs.getWorkingDirectory)
    if (path.toUri.getScheme == "file") {
      throw new RuntimeException("Could not read defaultFS - this may be caused by a missing Hadoop core-site.xml file")
    }
    path
  }

  lazy private val exportFs: FileSystem = exportPath.getFileSystem(to.conf)

  // sft should be shareable/the same from both datastores
  lazy private val sft: SimpleFeatureType = {
    val sft = from.ds.getSchema(typeName)
    if (sft == null) {
      throw new IllegalArgumentException(s"Schema '$typeName' does not exist in the source store")
    } else {
      val toSft = to.ds.getSchema(typeName)
      if (toSft == null) {
        throw new IllegalArgumentException(s"Schema '$typeName' does not exist in the destination store")
      } else if (SimpleFeatureTypes.compare(sft, toSft) != 0) {
        throw new IllegalArgumentException(s"Schema '$typeName' is not the same in the source and destination store")
      } else if (SimpleFeatureTypes.compareIndexConfigs(sft, toSft) != 0) {
        throw new IllegalArgumentException(s"Schema '$typeName' does not have compatible indices in the source and destination store")
      }
    }
    sft
  }

  lazy private val indexPairs: Seq[(GeoMesaFeatureIndex[_, _], GeoMesaFeatureIndex[_, _])] = {
    val all = from.ds.manager.indices(sft)
    val fromIndices = if (indices.isEmpty) { all } else {
      val builder = Seq.newBuilder[GeoMesaFeatureIndex[_, _]]
      indices.foreach { ident =>
        val filtered = all.filter(_.identifier.contains(ident))
        if (filtered.isEmpty) {
          throw new IllegalArgumentException(
            s"Index '$ident' does not exist in the schema. Available indices: ${all.map(_.identifier).mkString(", ")}")
        }
        logger.debug(s"Mapped identifier $ident to ${filtered.map(_.identifier).mkString(", ")}")
        builder ++= filtered
      }
      builder.result.distinct
    }
    fromIndices.map(from => from -> to.ds.manager.index(sft, from.identifier))
  }

  // these get passed into our index method calls - for partitioned schemas, it must be a Seq[Some[_]],
  // while for non-partitioned schemas it must always be Seq(None)
  lazy private val fromPartitions: Seq[Option[String]] = {
    if (sft.isPartitioned) {
      val builder = ListBuffer.empty[String]
      lazy val partitioning = TablePartition(from.ds, sft).get
      lazy val sf = new ScalaSimpleFeature(sft, "")
      builder ++= partitions.map {
        case PartitionName(name) => name
        case PartitionValue(value) =>
          sf.setAttribute(sft.getDtgIndex.get, value)
          val partition = partitioning.partition(sf)
          logger.debug(s"Generated partition $partition from value $value")
          partition
      }
      if (builder.isEmpty) {
        logger.debug("No partitions specified - loading all partitions from store")
        builder ++= indexPairs.flatMap(_._1.getPartitions)
      }
      builder.result.distinct.sorted.map(Option.apply)
    } else {
      if (partitions.nonEmpty) {
        throw new IllegalArgumentException("partitions are not applicable for a non-partitioned schema")
      }
      Seq(None)
    }
  }

  // planned copies
  lazy val plans: Set[CopyPlan] =
    fromPartitions.flatMap { partition =>
      indexPairs.map { case (fromIndex, _) =>
        CopyPlan(fromIndex.identifier, partition)
      }
    }.toSet

  /**
   * Execute the copy
   *
   * @return results
   */
  override def call(): Set[CopyResult] = call(false)

  /**
   * Execute the copy
   *
   * @param resume resume from a previously interrupted run, vs overwrite any existing output
   * @return results
   */
  def call(resume: Boolean): Set[CopyResult] = {
    val results = Collections.newSetFromMap(new ConcurrentHashMap[CopyResult, java.lang.Boolean]())
    CachedThreadPool.executor(options.tableConcurrency) { executor =>
      fromPartitions.foreach { partition =>
        indexPairs.foreach { case (fromIndex, toIndex) =>
          val partitionLogId = s"${partition.fold(s"index")(p => s"partition $p")} ${fromIndex.identifier}"
          val runnable: Runnable = () => {
            logger.info(s"Copying $partitionLogId")
            val result = copy(fromIndex, toIndex, partition, resume, partitionLogId)
            result.error match {
              case None =>
                logger.info(s"Bulk copy complete for $partitionLogId")
              case Some(e) =>
                logger.error(s"Error copying $partitionLogId: ${e.getMessage}")
                logger.debug(s"Error copying $partitionLogId", e)
            }
            results.add(result)
          }
          executor.submit(runnable)
        }
      }
    }
    results.asScala.toSet
  }

  /**
   * Copy a single index + partition
   *
   * @param fromIndex from index
   * @param toIndex to index
   * @param partition partition name - must be Some if schema is partitioned
   * @param resume use any partial results from a previous run, if present
   * @param partitionLogId identifier for log messages
   * @return result
   */
  private def copy(
      fromIndex: GeoMesaFeatureIndex[_, _],
      toIndex: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      resume: Boolean,
      partitionLogId: String): CopyResult = {
    val start = System.currentTimeMillis()
    val files = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())
    var fromTable: String = ""
    // lazy so that table, files and finish time are filled in appropriately
    lazy val result =
      CopyResult(fromIndex.identifier, partition, fromTable, files.asScala.toSeq, None, start, System.currentTimeMillis())
    try {
      fromTable = fromIndex.getTableName(partition)
      copy(fromTable, toIndex, partition, resume, partitionLogId, files)
      result
    } catch {
      // catch Throwable so NoClassDefFound still gets logged
      case e: Throwable => result.withError(e)
    }
  }

  /**
   * Copy a single index + partition
   *
   * @param fromTable from table
   * @param toIndex to index
   * @param partition partition name - must be Some if schema is partitioned
   * @param resume use any partial results from a previous run, if present
   * @param partitionLogId identifier for log messages
   * @param fileResults set to hold files that we've copied successfully
   * @return result
   */
  private def copy(
      fromTable: String,
      toIndex: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      resume: Boolean,
      partitionLogId: String,
      fileResults: java.util.Set[String]): Unit = {

    require(sft.isPartitioned == partition.isDefined) // sanity check - this should always be true due to our setup

    val completeMarker = new Path(exportPath, s"$fromTable.complete")
    if (exportFs.exists(completeMarker)) {
      if (resume) {
        logger.debug("Skipping already completed copy")
        return
      } else {
        exportFs.delete(completeMarker, false)
      }
    }

    val tableExportPath = new Path(exportPath, fromTable)
    val distcpPath = new Path(tableExportPath, "distcp.txt")
    val copyToDir = new Path(tableExportPath, "files")
    val cloneTable = s"${fromTable}_bc_tmp"

    logger.debug(s"Source table $fromTable (${from.tableOps.tableIdMap().get(fromTable)})")
    logger.debug(s"Export path $tableExportPath")

    if (resume && from.tableOps.exists(cloneTable)) {
      logger.debug(s"Using existing cloned table $cloneTable - ensuring table is offline")
      from.tableOps.offline(cloneTable, true)
    } else {
      // clone the table as we have to take it offline in order to export it
      // note that cloning is just a metadata op as it shares the underlying data files (until they change)
      logger.debug(s"Checking for existence and deleting any existing cloned table $cloneTable")
      from.ds.adapter.deleteTable(cloneTable) // no-op if table doesn't exist
      logger.debug(s"Cloning $fromTable to $cloneTable")
      from.tableOps.clone(fromTable, cloneTable, true, Collections.emptyMap(), Collections.emptySet()) // use 2.0 method for compatibility
      logger.debug(s"Taking $cloneTable offline")
      from.tableOps.offline(cloneTable, true)
    }

    if (resume && exportFs.exists(distcpPath) && exportFs.getFileStatus(distcpPath).getLen > 0) {
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
        logger.warn(s"Detected split mismatch between source ($fromTable) and destination ($toTable) for $partitionLogId")
      }
      logger.debug(s"Adding splits to destination table $toTable")
      to.tableOps.addSplits(toTable, splits)
    }

    val copyErrors = Collections.newSetFromMap(new ConcurrentHashMap[Throwable, java.lang.Boolean]())
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
                if (resume && exportFs.exists(copy) &&
                  path.getFileSystem(from.conf).getFileStatus(path).getLen == exportFs.getFileStatus(copy).getLen) {
                  logger.debug(s"Using existing copy of $path at $copy")
                } else {
                  consumer(path, copy)
                }
              } catch {
                // catch Throwable so NoClassDefFound still gets logged
                case e: Throwable =>
                  copyErrors.add(e)
                  logger.error(s"Failed to copy $path to $copy: ${e.getMessage}")
                  logger.debug(s"Failed to copy $path to $copy", e)
              }
            }
            executor.submit(runnable)
          }
        }
      }
    }

    if (options.distCp) {
      var inputPath = distcpPath
      val distCpFiles = ArrayBuffer.empty[String]
      if (resume) {
        logger.debug(s"Checking copy status of files in $distcpPath")
        inputPath = new Path(tableExportPath, "distcp-remaining.txt")
        WithClose(exportFs.create(inputPath, true)) { out =>
          distCpConsumer(1) { (path, _) =>
            logger.debug(s"Adding $path to distcp")
            out.writeUTF(s"$path\n")
            distCpFiles += path.getName
          }
        }
      } else {
        logger.debug(s"Checking file list at $distcpPath")
        distCpConsumer(1) { (path, _) =>
          distCpFiles += path.getName
        }
      }
      val job = new DistCp(from.conf, DistributedCopyOptions(inputPath, copyToDir)).execute()
      logger.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")
      while (!job.isComplete) {
        Thread.sleep(500)
      }
      if (job.isSuccessful) {
        logger.info(s"Successfully copied data to $copyToDir")
        fileResults.addAll(distCpFiles.asJava)
      } else {
        val msg = s"DistCp job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}"
        copyErrors.add(new RuntimeException(msg))
        logger.error(msg)
      }
    } else {
      distCpConsumer(options.fileConcurrency) { (path, copy) =>
        logger.debug(s"Copying $path to $copy")
        val fs = path.getFileSystem(from.conf)
        if (FileUtil.copy(fs, path, exportFs, copy, false, true, to.conf)) {
          fileResults.add(path.getName)
        } else {
          // consolidate error handling in the catch block
          throw new IOException(s"Failed to copy $path to $copy, copy returned false")
        }
      }
    }
    if (!copyErrors.isEmpty) {
      val e = new RuntimeException("Error copying data files")
      copyErrors.asScala.foreach(e.addSuppressed)
      throw e
    }

    logger.debug(s"Loading rfiles from $copyToDir to $toTable")
    val importDir = to.tableOps.importDirectory(copyToDir.toString).to(toTable)
    try { importDir.ignoreEmptyDir(true) } catch {
      case _: NoSuchMethodError => // accumulo 2.0, ignore
    }
    try { importDir.load() } catch {
      case e: IllegalArgumentException => logger.trace("Error importing directory:", e) // should mean empty dir
    }

    // create marker indicating this copy was successful
    logger.debug(s"Creating completion marker $completeMarker")
    exportFs.create(completeMarker).close()

    // cleanup
    logger.debug(s"Deleting export path $tableExportPath")
    exportFs.delete(tableExportPath, true)
    logger.debug(s"Deleting clone table $cloneTable")
    from.tableOps.delete(cloneTable)
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      CloseWithLogging(tryFrom.toOption)
      CloseWithLogging(tryTo.toOption)
    }
  }
}

object SchemaCopier {

  sealed trait ClusterCredentials
  case class ClusterPassword(password: String) extends ClusterCredentials
  case class ClusterKeytab(keytabPath: String) extends ClusterCredentials

  sealed trait PartitionId
  case class PartitionName(name: String) extends PartitionId
  case class PartitionValue(value: AnyRef) extends PartitionId

  /**
   * Connection info for an Accumulo cluster
   *
   * @param instanceName instance name
   * @param zookeepers zookeepers
   * @param user user
   * @param credentials credentials
   * @param catalog catalog table containing the feature type
   * @param configFiles additional hadoop *-site.xml files for configuring hdfs operations
   */
  case class ClusterConfig(
    instanceName: String, zookeepers: String, user: String, credentials: ClusterCredentials, catalog: String, configFiles: Seq[File])

  /**
   * Options
   *
   * @param tableConcurrency number of tables to copy in parallel
   * @param fileConcurrency number of files to copy in parallel, per table (when not using distcp)
   * @param distCp use hadoop distcp to copy files, instead of the hadoop client
   */
  case class CopyOptions(tableConcurrency: Int = 1, fileConcurrency: Int = 4, distCp: Boolean = false)

  /**
   * Planned copy operations
   *
   * @param index index id planned to copy
   * @param partition partition planned to copy
   */
  case class CopyPlan(index: String, partition: Option[String])

  /**
   * Result of a copy operation
   *
   * @param index index id being copied
   * @param partition partition being copied, if table is partitioned
   * @param table table being copied
   * @param files list of files that were successfully copied
   * @param error error, if any
   * @param start start of operation, in unix time
   * @param finish end of operation, in unix time
   */
  case class CopyResult(
      index: String,
      partition: Option[String],
      table: String,
      files: Seq[String],
      error: Option[Throwable],
      start: Long,
      finish: Long) {
    def withError(e: Throwable): CopyResult = copy(error = Option(e))
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
      credentials: ClusterCredentials,
      catalog: String,
      configs: Seq[File]
    ) extends Closeable {

    import scala.collection.JavaConverters._

    private val auth = credentials match {
      case ClusterPassword(password) => AccumuloDataStoreParams.PasswordParam.key -> password
      case ClusterKeytab(keytabPath) => AccumuloDataStoreParams.KeytabPathParam.key -> keytabPath
    }

    private val params = Map(
      AccumuloDataStoreParams.InstanceNameParam.key -> instance,
      AccumuloDataStoreParams.ZookeepersParam.key -> zk,
      AccumuloDataStoreParams.UserParam.key -> user,
      AccumuloDataStoreParams.CatalogParam.key -> catalog,
      auth
    )

    val conf = new Configuration()
    configs.foreach(f => conf.addResource(f.toURI.toURL))

    val ds: AccumuloDataStore =
      try { DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore] } catch {
        case NonFatal(e) => throw new IOException("Unable to load datastore:", e)
      }

    if (ds == null) {
      val maskedParams = params.map {
        case (AccumuloDataStoreParams.PasswordParam.key , _) => s"${AccumuloDataStoreParams.PasswordParam.key }=******"
        case (k, v) => s"$k=$v"
      }
      throw new IOException(s"Unable to load datastore using provided values: ${maskedParams.mkString(", ")}")
    }

    val tableOps: TableOperations = ds.connector.tableOperations()

    override def close(): Unit = ds.dispose()
  }

  private object Cluster {
    def apply(config: ClusterConfig): Cluster =
      new Cluster(config.instanceName, config.zookeepers, config.user, config.credentials, config.catalog, config.configFiles)
  }
}