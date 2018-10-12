/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import java.util.Locale
import java.util.regex.Pattern

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.coprocessor.AllCoprocessors
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.index.legacy._
import org.locationtech.geomesa.hbase.utils.HBaseVersions
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.index.ClientSideFiltering
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

object HBaseFeatureIndex extends HBaseIndexManagerType {

  private val DistributedJarNamePattern = Pattern.compile("^geomesa-hbase-distributed-runtime.*\\.jar$")

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseZ3IndexV1, HBaseXZ3Index, HBaseZ2Index, HBaseZ2IndexV1, HBaseXZ2Index, HBaseIdIndex,
      HBaseAttributeIndex, HBaseAttributeIndexV4, HBaseAttributeIndexV3, HBaseAttributeIndexV2, HBaseAttributeIndexV1)

  override val CurrentIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseXZ3Index, HBaseZ2Index, HBaseXZ2Index, HBaseIdIndex, HBaseAttributeIndex)

  override def indices(sft: SimpleFeatureType,
                       idx: Option[String] = None,
                       mode: IndexMode = IndexMode.Any): Seq[HBaseFeatureIndex] =
    super.indices(sft, idx, mode).asInstanceOf[Seq[HBaseFeatureIndex]]

  override def index(identifier: String): HBaseFeatureIndex =
    super.index(identifier).asInstanceOf[HBaseFeatureIndex]
}

trait HBaseFeatureIndex extends HBaseFeatureIndexType with ClientSideFiltering[Result] with LazyLogging {

  import org.locationtech.geomesa.hbase.HBaseSystemProperties.TableAvailabilityTimeout
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  protected val dataBlockEncoding: Option[DataBlockEncoding] = Some(DataBlockEncoding.FAST_DIFF)

  override def configure(sft: SimpleFeatureType, ds: HBaseDataStore, partition: Option[String]): String = {
    import HBaseFeatureIndex.DistributedJarNamePattern
    import HBaseSystemProperties.CoprocessorPath

    val table = super.configure(sft, ds, partition)

    val name = TableName.valueOf(table)

    WithClose(ds.connection.getAdmin) { admin =>
      if (!admin.tableExists(name)) {
        logger.debug(s"Creating table $name")

        val conf = admin.getConfiguration
        val compression = sft.userData[String](Configs.COMPRESSION_ENABLED).filter(_.toBoolean).map { _ =>
          // note: all compression types in HBase are case-sensitive and lower-cased
          val compressionType = sft.userData[String](Configs.COMPRESSION_TYPE).getOrElse("gz").toLowerCase(Locale.US)
          logger.debug(s"Setting compression '$compressionType' on table $name for feature ${sft.getTypeName}")
          Compression.getCompressionAlgorithmByName(compressionType)
        }

        val descriptor = new HTableDescriptor(name)
        HBaseColumnGroups(sft).foreach { case (group, _) =>
          val column = new HColumnDescriptor(group)
          compression.foreach(column.setCompressionType)
          HBaseVersions.addFamily(descriptor, column)
          dataBlockEncoding.foreach(column.setDataBlockEncoding)
        }

        if (ds.config.remoteFilter) {
          lazy val coprocessorUrl = ds.config.coprocessorUrl.orElse(CoprocessorPath.option.map(new Path(_))).orElse {
            try {
              // the jar should be under hbase.dynamic.jars.dir to enable filters, so look there
              val dir = new Path(conf.get("hbase.dynamic.jars.dir"))
              WithClose(dir.getFileSystem(conf)) { fs =>
                fs.listStatus(dir).collectFirst {
                  case s if DistributedJarNamePattern.matcher(s.getPath.getName).matches() => s.getPath
                }
              }
            } catch {
              case NonFatal(e) => logger.warn("Error checking dynamic jar path:", e); None
            }
          }

          def addCoprocessor(clazz: Class[_ <: Coprocessor], desc: HTableDescriptor): Unit = {
            val name = clazz.getCanonicalName
            if (!desc.getCoprocessors.contains(name)) {
              logger.debug(s"Using coprocessor path ${coprocessorUrl.orNull}")
              // TODO: Warn if the path given is different from paths registered in other coprocessors
              // if so, other tables would need updating
              HBaseVersions.addCoprocessor(desc, name, coprocessorUrl)
            }
          }

          // if the coprocessors are installed site-wide don't register them in the table descriptor
          val installed = Option(conf.get(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY))
          val names = installed.map(_.split(":").toSet).getOrElse(Set.empty[String])
          AllCoprocessors.foreach(c => if (!names.contains(c.getCanonicalName)) { addCoprocessor(c, descriptor) })
        }

        try { admin.createTableAsync(descriptor, getSplits(sft, partition).toArray) } catch {
          case _: org.apache.hadoop.hbase.TableExistsException => // ignore, another thread created it for us
        }
      }

      // wait for the table to come online
      if (!admin.isTableAvailable(name)) {
        val timeout = TableAvailabilityTimeout.toDuration.filter(_.isFinite())
        logger.debug(s"Waiting for table '$table' to become available with " +
            s"${timeout.map(t => s"a timeout of $t").getOrElse("no timeout")}")
        val stop = timeout.map(t => System.currentTimeMillis() + t.toMillis)
        while (!admin.isTableAvailable(name) && stop.forall(_ > System.currentTimeMillis())) {
          Thread.sleep(1000)
        }
      }
    }

    table
  }

  override def removeAll(sft: SimpleFeatureType, ds: HBaseDataStore): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    if (TablePartition.partitioned(sft)) {
      // partitioned indices can just drop the partitions
      delete(sft, ds, None)
    } else {
      getTableNames(sft, ds, None).par.foreach { name =>
        val tableName = TableName.valueOf(name)

        WithClose(ds.connection.getTable(tableName)) { table =>
          val scan = new Scan().setFilter(new KeyOnlyFilter)
          if (sft.isTableSharing) {
            scan.setRowPrefixFilter(sft.getTableSharingBytes)
          }
          ds.applySecurity(scan)
          val mutateParams = new BufferedMutatorParams(tableName)
          WithClose(table.getScanner(scan), ds.connection.getBufferedMutator(mutateParams)) { case (scanner, mutator) =>
            scanner.iterator.grouped(10000).foreach { result =>
              // TODO set delete visibilities
              val deletes = result.map(r => new Delete(r.getRow))
              mutator.mutate(deletes)
            }
          }
        }
      }
    }
  }

  override def delete(sft: SimpleFeatureType, ds: HBaseDataStore, partition: Option[String]): Unit = {
    // note: hbase tables are never shared
    WithClose(ds.connection.getAdmin) { admin =>
      getTableNames(sft, ds, partition).par.foreach { name =>
        val table = TableName.valueOf(name)
        if (admin.tableExists(table)) {
          admin.disableTableAsync(table)
          val timeout = TableAvailabilityTimeout.toDuration.filter(_.isFinite())
          logger.debug(s"Waiting for table '$table' to be disabled with " +
              s"${timeout.map(t => s"a timeout of $t").getOrElse("no timeout")}")
          val stop = timeout.map(t => System.currentTimeMillis() + t.toMillis)
          while (!admin.isTableDisabled(table) && stop.forall(_ > System.currentTimeMillis())) {
            Thread.sleep(1000)
          }
          // no async operation, but timeout can be controlled through hbase-site.xml "hbase.client.sync.wait.timeout.msec"
          admin.deleteTable(table)
        }
      }
    }

    // deletes the metadata
    super.delete(sft, ds, partition)
  }
}
