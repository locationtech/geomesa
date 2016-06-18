/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.api

import java.lang.Iterable
import java.util
import java.util.Date

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.vividsolutions.jts.geom.Geometry
import org.apache.hadoop.classification.InterfaceStability
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.filter.text.cql2.CQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.util.Z3UuidGenerator
import org.locationtech.geomesa.security.SecurityUtils
import org.opengis.feature.simple.SimpleFeature

@InterfaceStability.Unstable
class AccumuloGeoMesaIndex[T](ds: AccumuloDataStore,
                              serde: ValueSerializer[T],
                              view: SimpleFeatureView[T]
                             ) extends GeoMesaIndex[T] {

  val sft = view.getSimpleFeatureType

  if(!ds.getTypeNames.contains(sft.getTypeName)) {
    ds.createSchema(sft)
  }

  val fs = ds.getFeatureSource(sft.getTypeName)

  val writers =
    CacheBuilder.newBuilder().build(
      new CacheLoader[String, SimpleFeatureWriter] {
        override def load(k: String): SimpleFeatureWriter = {
          ds.getFeatureWriterAppend(k, Transaction.AUTO_COMMIT).asInstanceOf[SimpleFeatureWriter]
        }
      })

  override def query(query: GeoMesaQuery): Iterable[T] = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    import scala.collection.JavaConverters._

    fs.getFeatures(query.getFilter)
      .features()
      .map { f => serde.fromBytes(f.getAttribute(1).asInstanceOf[Array[Byte]]) }
      .toIterable.asJava
  }

  override def insert(id: String, value: T, geometry: Geometry, dtg: Date): String = {
    insert(id, value, geometry, dtg, null)
  }

  override def insert(value: T, geom: Geometry, dtg: Date): String = {
    val id = Z3UuidGenerator.createUuid(geom, dtg.getTime).toString
    insert(id, value, geom, dtg, null)
  }

  override def insert(id: String, value: T, geom: Geometry, dtg: Date, hints: util.Map[String, AnyRef]): String = {
    val bytes = serde.toBytes(value)
    val fw = writers.get(sft.getTypeName)
    val sf = fw.next()
    view.populate(sf, value, id, bytes, geom, dtg)
    setVisibility(sf, hints)
    fw.write()
    id
  }

  private def setVisibility(sf: SimpleFeature, hints: util.Map[String, AnyRef]): Unit = {
    if(hints != null && hints.containsKey(AccumuloGeoMesaIndex.VISIBILITY)) {
      val viz = hints.get(AccumuloGeoMesaIndex.VISIBILITY)
      sf.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, viz)
    }
  }

  override def supportedIndexes(): Array[IndexType] = Array(IndexType.SPATIOTEMPORAL, IndexType.RECORD)

  override def update(id: String, newValue: T, geometry: Geometry, dtg: Date): Unit = ???

  override def delete(id: String): Unit = fs.removeFeatures(CQL.toFilter(s"IN('$id'"))

  override def flush(): Unit = {
    // DO NOTHING - using AUTO_COMMIT
  }

  override def close(): Unit = {
    import scala.collection.JavaConversions._

    writers.asMap().values().foreach { w => w.close() }
  }

  def catalogTable() = ds.catalogTable

}

@InterfaceStability.Unstable
object AccumuloGeoMesaIndex {
  def build[T](name: String,
               zk: String,
               instanceId: String,
               user: String, pass: String,
               mock: Boolean,
               valueSerializer: ValueSerializer[T])
              (view: SimpleFeatureView[T] = new DefaultSimpleFeatureView[T](name)) = {
    import scala.collection.JavaConversions._
    val ds =
      DataStoreFinder.getDataStore(Map(
        AccumuloDataStoreParams.tableNameParam.key   -> name,
        AccumuloDataStoreParams.zookeepersParam.key  -> zk,
        AccumuloDataStoreParams.instanceIdParam.key  -> instanceId,
        AccumuloDataStoreParams.userParam.key        -> user,
        AccumuloDataStoreParams.passwordParam.key    -> pass,
        AccumuloDataStoreParams.mockParam.key        -> (if(mock) "TRUE" else "FALSE")
      )).asInstanceOf[AccumuloDataStore]
    new AccumuloGeoMesaIndex[T](ds, valueSerializer, view)
  }

  final val VISIBILITY = "visibility"
}
