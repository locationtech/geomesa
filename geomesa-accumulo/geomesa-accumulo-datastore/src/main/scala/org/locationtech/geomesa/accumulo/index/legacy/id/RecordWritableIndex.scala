/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.id

import java.nio.charset.StandardCharsets
import java.util.Collections

import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.index.conf.splitter.TableSplitter
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait RecordWritableIndex extends AccumuloFeatureIndex {

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String = {
    if (sft.isTableSharing) {
      (row, offset, length, feature) => new String(row, offset + 1, length - 1, StandardCharsets.UTF_8)
    } else {
      (row, offset, length, feature) => new String(row, offset, length, StandardCharsets.UTF_8)
    }
  }

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore, partition: Option[String]): String = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConverters._

    val table = super.configure(sft, ds, partition)

    AccumuloVersion.ensureTableExists(ds.connector, table)

    val prefix = sft.getTableSharingBytes
    val splits = TableSplitter.getSplits(sft, name, partition)
    val sortedSplits = splits.map(s => new Text(ByteArrays.concat(prefix, s))).toSet
    val splitsToAdd = sortedSplits -- ds.tableOps.listSplits(table).asScala.toSet
    if (splitsToAdd.nonEmpty) {
      ds.tableOps.addSplits(table, Collections.unmodifiableSortedSet(new java.util.TreeSet(splitsToAdd.asJava)))
    }

    // enable the row functor as the feature ID is stored in the Row ID
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
    ds.tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
    ds.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    table
  }
}
