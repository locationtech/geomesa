/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets

import com.google.common.primitives.Bytes
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.SimpleFeatureType

trait IdIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C] extends GeoMesaFeatureIndex[DS, F, W]
    with IndexAdapter[DS, F, W, R, C] with IdFilterStrategy[DS, F, W] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = IdIndex.Name

  override def supports(sft: SimpleFeatureType): Boolean = true

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    (wf) => {
      val row = Bytes.concat(sharing, wf.idBytes)
      Seq(createInsert(row, wf))
    }
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    (wf) => {
      val row = Bytes.concat(sharing, wf.idBytes)
      Seq(createDelete(row, wf))
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    if (sft.isTableSharing) {
      (row, offset, length) => new String(row, offset + 1, length - 1, StandardCharsets.UTF_8)
    } else {
      (row, offset, length) => new String(row, offset, length, StandardCharsets.UTF_8)
    }
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    val sharing = sft.getTableSharingBytes

    val splitter = sft.getTableSplitter.getOrElse(classOf[DefaultSplitter]).newInstance().asInstanceOf[TableSplitter]
    val splits = nonEmpty(splitter.getSplits(sft, name, sft.getTableSplitterOptions))

    val result = if (sharing.isEmpty) { splits } else {
      for (split <- splits) yield {
        Bytes.concat(sharing, split)
      }
    }

    // if not sharing, or the first feature in the table, drop the first split, which will otherwise be empty
    if (sharing.isEmpty || sharing.head == 0.toByte) {
      result.drop(1)
    } else {
      result
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W] = {

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    val prefix = sft.getTableSharingBytes

    filter.primary match {
      case None =>
        // allow for full table scans
        filter.secondary.foreach { f =>
          logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
        }
        scanPlan(sft, ds, filter, scanConfig(sft, ds, filter, Seq(rangePrefix(prefix)), filter.secondary, hints))

      case Some(primary) =>
        // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
        // intersect together all groups of ID Filters, producing a set of IDs
        val identifiers = IdFilterStrategy.intersectIdFilters(primary)
        explain(s"Extracted ID filter: ${identifiers.mkString(", ")}")
        val ranges = identifiers.toSeq.map { id =>
          rangeExact(Bytes.concat(prefix, id.getBytes(StandardCharsets.UTF_8)))
        }
        scanPlan(sft, ds, filter, scanConfig(sft, ds, filter, ranges, filter.secondary, hints))
    }
  }
}

object IdIndex {
  val Name = "id"
}
