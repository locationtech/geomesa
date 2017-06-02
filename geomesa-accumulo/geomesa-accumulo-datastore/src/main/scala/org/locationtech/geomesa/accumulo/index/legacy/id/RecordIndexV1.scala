/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.id

import com.google.common.primitives.Bytes
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature, EMPTY_COLQ}
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, RecordIndex}
import org.locationtech.geomesa.index.conf.{HexSplitter, TableSplitter}
import org.opengis.feature.simple.SimpleFeatureType

case object RecordIndexV1 extends AccumuloFeatureIndex with RecordWritableIndex with RecordQueryableIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val SFT_CF = new Text("SFT")

  override val name: String = "records"

  override val version: Int = 1

  override val serializedWithId: Boolean = true

  override val hasPrecomputedBins: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = true

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import scala.collection.JavaConversions._

    val prefix = sft.getTableSharingBytes
    val splitter = sft.getTableSplitter.getOrElse(classOf[HexSplitter]).newInstance().asInstanceOf[TableSplitter]
    val splits = splitter.getSplits(sft.getTableSplitterOptions)
    if (prefix.length == 0) { splits } else {
      splits.map(Bytes.concat(prefix, _))
    }
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(RecordIndex.getRowKey(rowIdPrefix, wf.feature.getID))
      wf.fullValuesWithId.foreach(value => mutation.put(SFT_CF, EMPTY_COLQ, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(RecordIndex.getRowKey(rowIdPrefix, wf.feature.getID))
      wf.fullValuesWithId.foreach(value => mutation.putDelete(SFT_CF, EMPTY_COLQ, value.vis))
      Seq(mutation)
    }
  }
}
