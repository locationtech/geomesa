/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.nio.ByteBuffer
import java.util.UUID
import collection.JavaConverters._

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder._
import org.geotools.data.{FeatureWriter => FW}
import org.joda.time.DateTime
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.geomesa.cassandra.{CassandraAppendFeatureWriterType, CassandraFeatureIndexType, CassandraFeatureWriterType, CassandraModifyFeatureWriterType}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class CassandraAppendFeatureWriter(sft: SimpleFeatureType, ds: CassandraDataStore, indices: Option[Seq[CassandraFeatureIndexType]])
      extends CassandraFeatureWriterType(sft, ds, indices) with CassandraAppendFeatureWriterType with CassandraFeatureWriter

class CassandraModifyFeatureWriter(sft: SimpleFeatureType,
                               ds: CassandraDataStore,
                               indices: Option[Seq[CassandraFeatureIndexType]],
                               val filter: Filter)
    extends CassandraFeatureWriterType(sft, ds, indices) with CassandraModifyFeatureWriterType with CassandraFeatureWriter

trait CassandraFeatureWriter extends CassandraFeatureWriterType {
  private val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

  override protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[String] = {
    tables
  }

  override protected def executeWrite(tname: String, writes: Seq[CassandraRow]): Unit = {
    writes.foreach(row => {
      ds.session.execute(row.qs.get.format(tname),  row.values.get:_*)
    })
  }

  override protected def executeRemove(tname: String, removes: Seq[CassandraRow]): Unit = {
    removes.foreach(row => {
      ds.session.execute(row.qs.get.format(tname))
    })
  }

  override def wrapFeature(feature: SimpleFeature): CassandraFeature = new CassandraFeature(feature, serializer)

}
