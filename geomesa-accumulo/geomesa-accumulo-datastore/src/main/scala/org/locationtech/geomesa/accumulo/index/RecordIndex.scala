/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.google.common.primitives.Bytes
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Range}
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.index.conf.{HexSplitter, TableSplitter}
import org.locationtech.geomesa.index.index.IdIndex
import org.opengis.feature.simple.SimpleFeatureType

case object RecordIndex extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with IdIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id

  override val name: String = "records"

  override val version: Int = 2

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = true

  override protected def queryThreads(ds: AccumuloDataStore): Int = ds.config.recordThreads

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit = {
    super.configure(sft, ds)
    val table = getTableName(sft.getTypeName, ds)
    // enable the row functor as the feature ID is stored in the Row ID
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import scala.collection.JavaConversions._

    val prefix = sft.getTableSharingBytes
    val splitter = sft.getTableSplitter.getOrElse(classOf[HexSplitter]).newInstance().asInstanceOf[TableSplitter]
    val splits = splitter.getSplits(sft.getTableSplitterOptions)
    if (prefix.length == 0) { splits } else {
      splits.map(Bytes.concat(prefix, _))
    }
  }
}
