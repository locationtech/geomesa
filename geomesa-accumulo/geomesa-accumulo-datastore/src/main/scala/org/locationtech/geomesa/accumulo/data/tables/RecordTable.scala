/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import java.nio.charset.StandardCharsets

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
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

// TODO: Implement as traits and cache results to gain flexibility and speed-up.
// https://geomesa.atlassian.net/browse/GEOMESA-344
object RecordTable extends GeoMesaTable {

  private val SFT_CF = new Text("SFT")

  override def supports(sft: SimpleFeatureType) = true

  override val suffix: String = "records"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val rowIdPrefix = sft.getTableSharingPrefix
    val putValues: (FeatureToWrite, Mutation) => Unit = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        (toWrite, mutation) => mutation.put(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility, toWrite.dataValue)
      case VisibilityLevel.Attribute =>
        (toWrite, mutation) => toWrite.perAttributeValues.foreach(key => mutation.put(key.cf, key.cq, key.vis, key.value))
    }
    (toWrite: FeatureToWrite) => {
      val mutation = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      putValues(toWrite, mutation)
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val rowIdPrefix = sft.getTableSharingPrefix
    val putValues: (FeatureToWrite, Mutation) => Unit = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        (toWrite, mutation) => mutation.putDelete(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility)
      case VisibilityLevel.Attribute =>
        (toWrite, mutation) => toWrite.perAttributeValues.foreach(key => mutation.putDelete(key.cf, key.cq, key.vis))
    }
    (toWrite: FeatureToWrite) => {
      val mutation = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      putValues(toWrite, mutation)
      Seq(mutation)
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = {
    val offset = sft.getTableSharingPrefix.length
    if (offset == 0) {
      (row) => new String(row, StandardCharsets.UTF_8)
    } else {
      (row) => new String(row.drop(offset), StandardCharsets.UTF_8)
    }
  }

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id

  override def configureTable(featureType: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    import scala.collection.JavaConversions._

    val prefix = featureType.getTableSharingPrefix
    val prefixFn = RecordTable.getRowKey(prefix, _: String)
    val splitterClazz = featureType.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER, classOf[HexSplitter].getCanonicalName).asInstanceOf[String]
    val clazz = Class.forName(splitterClazz)
    val splitter = clazz.newInstance().asInstanceOf[TableSplitter]
    val splitterOptions = featureType.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS, Map.empty[String, String]).asInstanceOf[Map[String, String]]
    val splits = splitter.getSplits(splitterOptions)
    val sortedSplits = splits.map(_.toString).map(prefixFn).map(new Text(_)).toSet
    val splitsToAdd = sortedSplits -- tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    // enable the row functor as the feature ID is stored in the Row ID
    tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
    tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}
