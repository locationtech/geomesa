/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.id

import java.nio.charset.StandardCharsets

import com.google.common.collect.ImmutableSortedSet
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{AccumuloFeatureIndex, RecordIndex}
import org.locationtech.geomesa.index.conf.{HexSplitter, TableSplitter}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

trait RecordWritableIndex extends AccumuloFeatureIndex {

  import RecordIndex.getRowKey

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    if (sft.isTableSharing) {
      (row, offset, length) => new String(row, offset + 1, length - 1, StandardCharsets.UTF_8)
    } else {
      (row, offset, length) => new String(row, offset, length, StandardCharsets.UTF_8)
    }
  }

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    super.configure(sft, ds)
    val table = getTableName(sft.getTypeName, ds)

    AccumuloVersion.ensureTableExists(ds.connector, table)

    val prefix = sft.getTableSharingPrefix
    val prefixFn = getRowKey(prefix, _: String)
    val splitter = sft.getTableSplitter.getOrElse(classOf[HexSplitter]).newInstance().asInstanceOf[TableSplitter]
    val splits = splitter.getSplits(sft.getTableSplitterOptions)
    val sortedSplits = splits.map(new String(_, StandardCharsets.UTF_8)).map(prefixFn).map(new Text(_)).toSet
    val splitsToAdd = sortedSplits -- ds.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ds.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    // enable the row functor as the feature ID is stored in the Row ID
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    ds.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}
