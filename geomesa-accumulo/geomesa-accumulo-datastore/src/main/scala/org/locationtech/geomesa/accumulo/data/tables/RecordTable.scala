/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import com.google.common.collect.ImmutableSortedSet
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

// TODO: Implement as traits and cache results to gain flexibility and speed-up.
// https://geomesa.atlassian.net/browse/GEOMESA-344
object RecordTable extends GeoMesaTable {

  override def supports(sft: SimpleFeatureType) = true

  override val suffix: String = "records"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.put(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility, toWrite.dataValue)
      Seq(m)
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.putDelete(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility)
      Seq(m)
    }
  }

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id

  override def configureTable(featureType: SimpleFeatureType, recordTable: String, tableOps: TableOperations): Unit = {
    import scala.collection.JavaConversions._

    val prefix = featureType.getTableSharingPrefix
    val prefixFn = RecordTable.getRowKey(prefix, _: String)
    val splitterClazz = featureType.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER, classOf[HexSplitter].getCanonicalName).asInstanceOf[String]
    val clazz = Class.forName(splitterClazz)
    val splitter = clazz.newInstance().asInstanceOf[TableSplitter]
    val splitterOptions = featureType.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS, Map.empty[String, String]).asInstanceOf[Map[String, String]]
    val splits = splitter.getSplits(splitterOptions)
    val sortedSplits = splits.map(_.toString).map(prefixFn).map(new Text(_)).toSet
    val splitsToAdd = sortedSplits -- tableOps.listSplits(recordTable).toSet
    if (splitsToAdd.nonEmpty) {
      tableOps.addSplits(recordTable, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    // enable the row functor as the feature ID is stored in the Row ID
    tableOps.setProperty(recordTable, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
    tableOps.setProperty(recordTable, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    tableOps.setProperty(recordTable, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
    tableOps.setProperty(recordTable, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
  }
}
