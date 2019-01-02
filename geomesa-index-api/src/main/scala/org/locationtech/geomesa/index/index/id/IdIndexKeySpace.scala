/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object IdIndexKeySpace extends IndexKeySpace[Set[Array[Byte]], Array[Byte]]()(ByteArrays.ByteOrdering)
    with IdIndexKeySpace

trait IdIndexKeySpace extends IndexKeySpace[Set[Array[Byte]], Array[Byte]] {

  override def supports(sft: SimpleFeatureType): Boolean = true

  // note: technically this doesn't match the index key, but it's only
  // used for extracting the feature ID so it works out
  override def indexKeyByteLength: Int = 0

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): SimpleFeature => Seq[Array[Byte]] =
    toBytesKey(GeoMesaFeatureIndex.idToBytes(sft))

  override def toIndexKeyBytes(sft: SimpleFeatureType, lenient: Boolean): ToIndexKeyBytes = getIdAsBytes

  override def getIndexValues(sft: SimpleFeatureType,
                              filter: Filter,
                              explain: Explainer): Set[Array[Byte]] = {
    // Multiple sets of IDs in a ID Filter are ORs. ANDs of these call for the intersection to be taken.
    // intersect together all groups of ID Filters, producing a set of IDs
    val identifiers = IdFilterStrategy.intersectIdFilters(filter)
    explain(s"Extracted ID filter: ${identifiers.mkString(", ")}")
    val serializer = GeoMesaFeatureIndex.idToBytes(sft)
    identifiers.map(serializer.apply)
  }

  override def getRanges(values: Set[Array[Byte]], multiplier: Int): Iterator[ScanRange[Array[Byte]]] =
    values.iterator.map(SingleRowRange.apply)

  override def getRangeBytes(ranges: Iterator[ScanRange[Array[Byte]]],
                             prefixes: Seq[Array[Byte]],
                             tier: Boolean): Iterator[ByteRange] = {
    if (prefixes.isEmpty) {
      ranges.map {
        case SingleRowRange(row) => SingleRowByteRange(row)
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case SingleRowRange(row) => prefixes.map(p => SingleRowByteRange(ByteArrays.concat(p, row)))
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  override def useFullFilter(values: Option[Set[Array[Byte]]],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = false

  private def toBytesKey(toBytes: (String) => Array[Byte])(feature: SimpleFeature): Seq[Array[Byte]] =
    Seq(toBytes(feature.getID))

  private def getIdAsBytes(prefix: Seq[Array[Byte]], feature: SimpleFeature, suffix: Array[Byte]): Seq[Array[Byte]] = {
    // note: suffix contains feature ID, so we don't need to add anything else
    val length = prefix.map(_.length).sum + suffix.length
    val result = Array.ofDim[Byte](length)
    var i = 0
    prefix.foreach { p =>
      System.arraycopy(p, 0, result, i, p.length)
      i += p.length
    }
    System.arraycopy(suffix, 0, result, i, suffix.length)
    Seq(result)
  }
}
