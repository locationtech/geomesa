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
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloBulkCopyCommand.AccumuloBulkCopyParams
import org.locationtech.geomesa.accumulo.util.SchemaCopier
import org.locationtech.geomesa.accumulo.util.SchemaCopier._
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.ParameterValidators.PositiveInteger
import org.locationtech.geomesa.utils.io.WithClose

import java.io.File

/**
 * Copy a partitioned table out of one feature type and into an identical feature type
 */
class AccumuloBulkCopyCommand extends Command with StrictLogging {

  import scala.collection.JavaConverters._

  override val name = "bulk-copy"
  override val params = new AccumuloBulkCopyParams()

  override def execute(): Unit = {
    val fromCluster = {
      val auth = if (params.fromKeytab != null) { ClusterKeytab(params.fromKeytab) } else { ClusterPassword(params.fromPassword) }
      val configs = if (params.fromConfigs == null) { Seq.empty } else { params.fromConfigs.asScala.toSeq }
      ClusterConfig(params.fromInstance, params.fromZookeepers, params.fromUser, auth, params.fromCatalog, configs)
    }
    val toCluster = {
      val auth = if (params.toKeytab != null) { ClusterKeytab(params.toKeytab) } else { ClusterPassword(params.toPassword) }
      val configs = if (params.toConfigs == null) { Seq.empty } else { params.toConfigs.asScala.toSeq }
      ClusterConfig(params.toInstance, params.toZookeepers, params.toUser, auth, params.toCatalog, configs)
    }
    val indices = if (params.indices == null) { Seq.empty } else { params.indices.asScala.toSeq }
    val partitions: Seq[PartitionId] =
      Option(params.partitions).fold(Seq.empty[PartitionId])(_.asScala.map(PartitionName.apply).toSeq) ++
        Option(params.partitionValues).fold(Seq.empty[PartitionId])(_.asScala.map(PartitionValue.apply).toSeq)
    val opts = CopyOptions(params.tableThreads, params.fileThreads, params.distCp)

    WithClose(new SchemaCopier(fromCluster, toCluster, params.featureName, params.exportPath, indices, partitions, opts)) { copier =>
      try {
        copier.call(params.resume)
      } catch {
        case e: IllegalArgumentException => throw new ParameterException(e.getMessage)
      }
    }

  }
}

object AccumuloBulkCopyCommand extends LazyLogging {

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
