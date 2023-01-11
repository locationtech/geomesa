/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.EmptyIndex.EmptyKeySpace
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
 * Placeholder index for scans that won't return any results (i.e. Filter.EXCLUDE)
 *
 * @param ds data store
 * @param sft simple feature type stored in this index
 */
class EmptyIndex(ds: GeoMesaDataStore[_], sft: SimpleFeatureType)
    extends GeoMesaFeatureIndex[String, String](ds, sft, EmptyIndex.name, EmptyIndex.version, Seq.empty, IndexMode.Read) {
  override val keySpace: IndexKeySpace[String, String] = new EmptyKeySpace(sft)
  override def tieredKeySpace: Option[IndexKeySpace[_, _]] = None
  override def getFilterStrategy(filter: Filter, transform: Option[SimpleFeatureType]): Option[FilterStrategy] = None
}

object EmptyIndex extends NamedIndex {

  override val name: String = "none"
  override val version: Int = 0

  class EmptyKeySpace(val sft: SimpleFeatureType) extends IndexKeySpace[String, String] {

    override val attributes: Seq[String] = Seq.empty
    override val indexKeyByteLength: Either[(Array[Byte], Int, Int) => Int, Int] = Right(0)
    override val sharing: Array[Byte] = Array.empty
    override val sharding: ShardStrategy = NoShardStrategy

    override def toIndexKey(
        feature: WritableFeature,
        tier: Array[Byte],
        id: Array[Byte],
        lenient: Boolean): RowKeyValue[String] = throw new NotImplementedError()

    override def getIndexValues(filter: Filter, explain: Explainer): String = null
    override def getRanges(values: String, multiplier: Int): Iterator[ScanRange[String]] = Iterator.empty
    override def getRangeBytes(ranges: Iterator[ScanRange[String]], tier: Boolean): Iterator[ByteRange] = Iterator.empty
    override def useFullFilter(values: Option[String], config: Option[GeoMesaDataStoreConfig],hints: Hints): Boolean = false
  }
}
