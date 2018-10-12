/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import java.io.IOException

import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client.{KuduSession, OperationResponse}
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.kudu._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

abstract class KuduFeatureWriter(val sft: SimpleFeatureType,
                                  val ds: KuduDataStore,
                                  val indices: Seq[KuduFeatureIndexType],
                                  session: KuduSession,
                                  val filter: Filter,
                                  val partition: TablePartition) extends KuduFeatureWriterType {

  import KuduFeatureWriter.existingPartitionError
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val dtgIndex = sft.getDtgIndex
  private val toBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

  // track partitions written by this instance
  private val partitions = scala.collection.mutable.Set.empty[String]

  override protected def wrapFeature(feature: SimpleFeature): KuduFeature = new KuduFeature(feature, dtgIndex, toBin)

  override protected def createMutator(table: String): KuduSession = session

  override protected def executeWrite(session: KuduSession, writes: Seq[WriteOperation]): Unit = {
    writes.foreach { write =>
      // if we haven't seen this partition before, try to add it
      // it's hard (impossible?) to inspect existing partitions through the kudu API,
      // so we just add them and suppress duplicate partition errors
      if (partitions.add(write.partition)) {
        write.partitioning().foreach { case Partitioning(table, alter) =>
          try { ds.client.alterTable(table, alter) } catch {
            case e if existingPartitionError(e) =>
              logger.debug(s"Tried to create a partition to table $table but it already exists: ", e)
          }
        }
      }
      session.apply(write.op)
    }
  }

  // note: we don't worry about partitions for removes, as presumably they already exist...
  override protected def executeRemove(session: KuduSession, removes: Seq[WriteOperation]): Unit =
    removes.foreach(r => session.apply(r.op))

  // we have to flush/close the session here, as it doesn't implement flushable or closeable
  override def flush(): Unit = handleErrors(session.flush())
  override def close(): Unit = handleErrors(session.close())

  private def handleErrors(response: java.util.List[OperationResponse]): Unit = {
    import scala.collection.JavaConverters._

    val errors = response.asScala.collect { case row if row.hasRowError => row.getRowError }
    if (errors.nonEmpty) {
      val e = new RuntimeException("Error closing session")
      errors.foreach(error => e.addSuppressed(new IOException(error.toString)))
      throw e
    }
  }
}

object KuduFeatureWriter {

  def existingPartitionError(e: Throwable): Boolean = // TODO more robust matching?
    e.getMessage != null && e.getMessage.startsWith("New range partition conflicts with existing range partition")

  class KuduFeatureWriterFactory(ds: KuduDataStore) extends KuduFeatureWriterFactoryType {
    override def createFeatureWriter(sft: SimpleFeatureType,
                                     indices: Seq[KuduFeatureIndexType],
                                     filter: Option[Filter]): FlushableFeatureWriter = {
      val session = ds.client.newSession()
      // increase the number of mutations that we can buffer
      session.setMutationBufferSpace(KuduSystemProperties.MutationBufferSpace.toInt.get)
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

      (TablePartition(ds, sft), filter) match {
        case (None, None) =>
          new KuduFeatureWriter(sft, ds, indices, session, null, null)
              with KuduTableFeatureWriterType with KuduAppendFeatureWriterType

        case (None, Some(f)) =>
          new KuduFeatureWriter(sft, ds, indices, session, f, null)
              with KuduTableFeatureWriterType with KuduModifyFeatureWriterType

        case (Some(p), None) =>
          new KuduFeatureWriter(sft, ds, indices, session, null, p)
              with KuduPartitionedFeatureWriterType with KuduAppendFeatureWriterType

        case (Some(p), Some(f)) =>
          new KuduFeatureWriter(sft, ds, indices, session, f, p)
              with KuduPartitionedFeatureWriterType with KuduModifyFeatureWriterType
      }
    }
  }
}
