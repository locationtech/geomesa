/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import org.apache.kudu.client.KuduSession
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.kudu._
import org.locationtech.geomesa.kudu.utils.RichKuduClient.SessionHolder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait KuduFeatureWriter extends KuduFeatureWriterType {

  import KuduFeatureWriter.existingPartitionError
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  protected def session: KuduSession

  private val dtgIndex = sft.getDtgIndex
  private val toBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

  // track partitions written by this instance
  private val partitions = scala.collection.mutable.Set.empty[String]

  override protected def wrapFeature(feature: SimpleFeature): KuduFeature = new KuduFeature(feature, dtgIndex, toBin)

  override protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[SessionHolder] = {
    val holder = SessionHolder(session)
    IndexedSeq.fill(tables.size)(holder) // TODO does it matter this will be flushed/closed once per table?
  }

  override protected def executeWrite(holder: SessionHolder, writes: Seq[WriteOperation]): Unit = {
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
      holder.session.apply(write.op)
    }
  }

  // note: we don't worry about partitions for removes, as presumably they already exist...
  override protected def executeRemove(holder: SessionHolder, removes: Seq[WriteOperation]): Unit =
    removes.foreach(r => holder.session.apply(r.op))
}

object KuduFeatureWriter {

  def existingPartitionError(e: Throwable): Boolean = // TODO more robust matching?
    e.getMessage != null && e.getMessage.startsWith("New range partition conflicts with existing range partition")

  class KuduAppendFeatureWriter(sft: SimpleFeatureType,
                                ds: KuduDataStore,
                                indices: Option[Seq[KuduFeatureIndexType]],
                                override protected val session: KuduSession)
      extends KuduFeatureWriterType(sft, ds, indices) with KuduAppendFeatureWriterType with KuduFeatureWriter

  class KuduModifyFeatureWriter(sft: SimpleFeatureType,
                                ds: KuduDataStore,
                                indices: Option[Seq[KuduFeatureIndexType]],
                                val filter: Filter,
                                override protected val session: KuduSession)
      extends KuduFeatureWriterType(sft, ds, indices) with KuduModifyFeatureWriterType with KuduFeatureWriter
}