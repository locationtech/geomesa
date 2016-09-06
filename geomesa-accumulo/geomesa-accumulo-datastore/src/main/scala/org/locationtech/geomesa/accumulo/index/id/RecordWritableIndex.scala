/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.id

import com.google.common.base.Charsets
import com.google.common.collect.ImmutableSortedSet
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

// TODO: Implement as traits and cache results to gain flexibility and speed-up.
// https://geomesa.atlassian.net/browse/GEOMESA-344
trait RecordWritableIndex extends AccumuloWritableIndex {

  import RecordIndex.getRowKey

  private val SFT_CF = new Text("SFT")

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val rowIdPrefix = sft.getTableSharingPrefix
    if (sft.getSchemaVersion < 9) {
      (wf: WritableFeature) => {
        val mutation = new Mutation(getRowKey(rowIdPrefix, wf.feature.getID))
        wf.fullValues.foreach(value => mutation.put(SFT_CF, EMPTY_COLQ, value.vis, value.value))
        Seq(mutation)
      }
    } else {
      (wf: WritableFeature) => {
        val mutation = new Mutation(getRowKey(rowIdPrefix, wf.feature.getID))
        wf.fullValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
        Seq(mutation)
      }
    }
  }

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val rowIdPrefix = sft.getTableSharingPrefix
    if (sft.getSchemaVersion < 9) {
      (wf: WritableFeature) => {
        val mutation = new Mutation(getRowKey(rowIdPrefix, wf.feature.getID))
        wf.fullValues.foreach(value => mutation.putDelete(SFT_CF, EMPTY_COLQ, value.vis))
        Seq(mutation)
      }
    } else {
      (wf: WritableFeature) => {
        val mutation = new Mutation(getRowKey(rowIdPrefix, wf.feature.getID))
        wf.fullValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
        Seq(mutation)
      }
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = {
    val offset = sft.getTableSharingPrefix.length
    (row: Text) => new String(row.getBytes, offset, row.getLength - offset, Charsets.UTF_8)
  }

  override def configure(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
    import scala.collection.JavaConversions._

    val table = ops.getTableName(sft.getTypeName, this)
    AccumuloVersion.ensureTableExists(ops.connector, table)
    val prefix = sft.getTableSharingPrefix
    val prefixFn = getRowKey(prefix, _: String)
    val splitterClazz = sft.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER, classOf[HexSplitter].getCanonicalName).asInstanceOf[String]
    val clazz = Class.forName(splitterClazz)
    val splitter = clazz.newInstance().asInstanceOf[TableSplitter]
    val splitterOptions = sft.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS, Map.empty[String, String]).asInstanceOf[Map[String, String]]
    val splits = splitter.getSplits(splitterOptions)
    val sortedSplits = splits.map(_.toString).map(prefixFn).map(new Text(_)).toSet
    val splitsToAdd = sortedSplits -- ops.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ops.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    // enable the row functor as the feature ID is stored in the Row ID
    ops.tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
    ops.tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    ops.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}
