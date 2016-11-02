/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets

import com.google.common.primitives.Bytes
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.SimpleFeatureType

trait IdIndex[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, R] extends GeoMesaFeatureIndex[DS, F, W, Q]
    with IndexAdapter[DS, F, W, Q, R] with IdFilterStrategy[DS, F, W, Q] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "id"

  override def supports(sft: SimpleFeatureType): Boolean = true

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => W = {
    val sharing = sft.getTableSharingBytes
    (wf) => {
      val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
      val row = Bytes.concat(sharing, id)
      createInsert(row, wf)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => W = {
    val sharing = sft.getTableSharingBytes
    (wf) => {
      val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
      val row = Bytes.concat(sharing, id)
      createDelete(row, wf)
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = {
    if (sft.isTableSharing) {
      (row: Array[Byte]) => new String(row, 1, row.length - 1, StandardCharsets.UTF_8)
    } else {
      (row: Array[Byte]) => new String(row, StandardCharsets.UTF_8)
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W, Q],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W, Q] = {
    import org.locationtech.geomesa.filter.FilterHelper._

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
        scanPlan(sft, ds, filter, hints, Seq(rangePrefix(prefix)), filter.filter)

      case Some(primary) =>
        // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
        // intersect together all groups of ID Filters, producing a set of IDs
        val identifiers = IdFilterStrategy.intersectIdFilters(primary)
        explain(s"Extracted ID filter: ${identifiers.mkString(", ")}")
        val ranges = identifiers.toSeq.map { id =>
          rangeExact(Bytes.concat(prefix, id.getBytes(StandardCharsets.UTF_8)))
        }
        scanPlan(sft, ds, filter, hints, ranges, filter.filter)
    }
  }
}
