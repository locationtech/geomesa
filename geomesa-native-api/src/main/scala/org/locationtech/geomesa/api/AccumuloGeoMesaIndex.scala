/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.api

import java.lang.Iterable
import java.util.Date

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.vividsolutions.jts.geom.Geometry
import org.apache.hadoop.classification.InterfaceStability
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.cql2.CQL
import org.locationtech.geomesa.accumulo.data.tables.{RecordTable, Z3Table}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.util.Z3UuidGenerator
import org.locationtech.geomesa.utils.geotools.{Conversions, SftBuilder}

@InterfaceStability.Unstable
class AccumuloGeoMesaIndex[T](ds: AccumuloDataStore,
                              fname: String,
                              serde: ValueSerializer[T]) extends GeoMesaIndex[T] {
  private val ff = CommonFactoryFinder.getFilterFactory2

  if(!ds.getTypeNames.contains(fname)) {
    val sft =
      new SftBuilder()
        .date("dtg", default = true, index = true)
        .bytes("payload")
        .geometry("geom", default = true)
        .withIndexes(List(Z3Table.suffix, RecordTable.suffix))
        .userData("geomesa.mixed.geometries","true")
        .build(fname)
    ds.createSchema(sft)
  }

  val fs = ds.getFeatureSource(fname)

  val writers =
    CacheBuilder.newBuilder().build(
      new CacheLoader[String, SimpleFeatureWriter] {
        override def load(k: String): SimpleFeatureWriter = {
          ds.getFeatureWriterAppend(k, Transaction.AUTO_COMMIT).asInstanceOf[SimpleFeatureWriter]
        }
      })

  override def query(query: GeoMesaQuery): Iterable[T] = {
    import Conversions._

    import scala.collection.JavaConverters._

    fs.getFeatures(query.getFilter)
      .features()
      .map { f => serde.fromBytes(f.getAttribute(1).asInstanceOf[Array[Byte]]) }
      .toIterable.asJava
  }

  override def insert(value: T, geom: Geometry, dtg: Date): String = {
    val bytes = serde.toBytes(value)
    val fw = writers.get(fname)
    val sf = fw.next()
    val id = Z3UuidGenerator.createUuid(geom, dtg.getTime).toString
    sf.setDefaultGeometry(geom)
    sf.setAttribute(0, dtg)
    sf.setAttribute(1, bytes)
    sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
    fw.write()
    id
  }

  override def supportedIndexes(): Array[IndexType] = Array(IndexType.SPATIOTEMPORAL, IndexType.RECORD)

  override def update(id: String, newValue: T, geometry: Geometry, dtg: Date): Unit = ???

  override def insert(id: String, value: T, geometry: Geometry, dtg: Date): Unit = {
    val bytes = serde.toBytes(value)
    val fw = writers.get(fname)
    val sf = fw.next()
    sf.setDefaultGeometry(geometry)
    sf.setAttribute(0, dtg)
    sf.setAttribute(1, bytes)
    sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
    fw.write()
  }

  override def delete(id: String): Unit = fs.removeFeatures(CQL.toFilter(s"IN('$id'"))
}

@InterfaceStability.Unstable
object AccumuloGeoMesaIndex {
  def build[T](name: String, zk: String, instanceId: String, user: String, pass: String, mock: Boolean,
               valueSerializer: ValueSerializer[T]) = {
    import scala.collection.JavaConversions._
    val ds =
      DataStoreFinder.getDataStore(Map(
        AccumuloDataStoreParams.tableNameParam.key -> name,
        AccumuloDataStoreParams.zookeepersParam.key -> zk,
        AccumuloDataStoreParams.instanceIdParam.key -> instanceId,
        AccumuloDataStoreParams.userParam.key -> user,
        AccumuloDataStoreParams.passwordParam.key -> pass,
        AccumuloDataStoreParams.mockParam.key -> (if(mock) "TRUE" else "FALSE")
      )).asInstanceOf[AccumuloDataStore]
    new AccumuloGeoMesaIndex[T](ds, name, valueSerializer)
  }
}
