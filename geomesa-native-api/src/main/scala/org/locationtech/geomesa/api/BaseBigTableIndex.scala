/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.api

import java.lang.Iterable
import java.util
import java.util.Date

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.locationtech.jts.geom.Geometry
import org.apache.hadoop.classification.InterfaceStability
import org.geotools.data.Transaction
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.curve.TimePeriod
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SchemaBuilder
import org.locationtech.geomesa.utils.uuid.Z3UuidGenerator
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConverters._

@InterfaceStability.Unstable
@deprecated("Will be removed without replacement")
abstract class BaseBigTableIndex[T](protected val ds: GeoMesaDataStore[_],
                                    name: String,
                                    serde: ValueSerializer[T],
                                    view: SimpleFeatureView[T]) extends GeoMesaIndex[T] {

  protected[this] val sft = BaseBigTableIndex.buildSimpleFeatureType(name)(view)

  if (!ds.getTypeNames.contains(sft.getTypeName)) {
    ds.createSchema(sft)
  }

  protected[this] val fs = ds.getFeatureSource(sft.getTypeName)

  protected[this] val writers =
    Caffeine.newBuilder().build(
      new CacheLoader[String, FlushableFeatureWriter] {
        override def load(k: String): FlushableFeatureWriter = {
          ds.getFeatureWriterAppend(k, Transaction.AUTO_COMMIT)
        }
      })

  override def query(query: GeoMesaQuery): Iterable[T] = {
    import scala.collection.JavaConverters._

    SelfClosingIterator(fs.getFeatures(query.getFilter).features)
      .map { f => serde.fromBytes(f.getAttribute(1).asInstanceOf[Array[Byte]]) }
      .toIterable.asJava
  }

  override def insert(id: String, value: T, geometry: Geometry, dtg: Date): String = {
    insert(id, value, geometry, dtg, null)
  }

  override def insert(value: T, geom: Geometry, dtg: Date): String = {
    val id = Z3UuidGenerator.createUuid(geom, dtg.getTime, TimePeriod.Week).toString
    insert(id, value, geom, dtg, null)
  }

  override def insert(id: String, value: T, geom: Geometry, dtg: Date, hints: util.Map[String, AnyRef]): String = {
    val bytes = serde.toBytes(value)
    val fw = writers.get(sft.getTypeName)
    val sf = fw.next()
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf.setAttribute("geom", geom)
    sf.setAttribute("dtg", dtg)
    sf.setAttribute("payload", bytes)
    sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
    view.populate(sf, value, id, bytes, geom, dtg)
    setVisibility(sf, hints)
    fw.write()
    id
  }

  private def setVisibility(sf: SimpleFeature, hints: util.Map[String, AnyRef]): Unit = {
    if (hints != null && hints.containsKey(BaseBigTableIndex.VISIBILITY)) {
      val viz = hints.get(BaseBigTableIndex.VISIBILITY)
      sf.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, viz)
    }
  }

  override def supportedIndexes(): Array[IndexType] =
    Array(IndexType.SPATIOTEMPORAL, IndexType.SPATIAL, IndexType.RECORD)

  override def update(id: String, newValue: T, geometry: Geometry, dtg: Date): Unit = ???

  override def delete(id: String): Unit = fs.removeFeatures(ECQL.toFilter(s"IN('$id')"))

  // should remove the index (SFT) as well as the associated Accumulo tables, if appropriate
  override def removeSchema(): Unit = ds.removeSchema(sft.getTypeName)

  override def close(): Unit = {
    import scala.collection.JavaConversions._

    writers.asMap().values().foreach {
      _.close()
    }
    ds.dispose()
  }

  protected[this] def catalogTable() = ds.config.catalog

}

@InterfaceStability.Unstable
object BaseBigTableIndex {
  private def buildSimpleFeatureType[T](name: String)
                                       (view: SimpleFeatureView[T] = new DefaultSimpleFeatureView[T]()) = {
    val builder = SchemaBuilder.builder()
      .addDate("dtg", default = true)
      .addBytes("payload")
      .addMixedGeometry("geom", default = true)

    view.getExtraAttributes.asScala.foreach(builder.addAttribute)

    builder.build(name)
  }

  final val VISIBILITY = "visibility"
}
