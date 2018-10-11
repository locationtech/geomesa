/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Range}
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType

case object RecordIndex extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with IdIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range, ScanConfig] {

  def getRowKey(sft: SimpleFeatureType): (Array[Byte], String) => Array[Byte] = {
    val idToBytes = GeoMesaFeatureIndex.idToBytes(sft)
    (prefix, id) => ByteArrays.concat(prefix, idToBytes(id))
  }

  override val version: Int = 3

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = true

  override protected def queryThreads(ds: AccumuloDataStore): Int = ds.config.recordThreads

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore, partition: Option[String]): String = {
    val table = super.configure(sft, ds, partition)
    // enable the row functor as the feature ID is stored in the Row ID
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    table
  }
}
