/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.geomesa.GeoMesaSparkKryoRegistratorEndpoint
import org.apache.spark.serializer.KryoRegistrator
import org.geotools.api.data.DataStore
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.feature.simple.SimpleFeatureImpl
import org.locationtech.geomesa.features.ScalaSimpleFeature.{ImmutableSimpleFeature, LazyImmutableSimpleFeature, LazyMutableSimpleFeature}
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
<<<<<<< HEAD
import scala.util.Try
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import scala.util.hashing.MurmurHash3

class GeoMesaSparkKryoRegistrator extends KryoRegistrator with LazyLogging {

  override def registerClasses(kryo: Kryo): Unit = {
    registerSimpleFeatureClasses(kryo)
    if (isUsingSedona) {
      registerSedonaClasses(kryo)
    }
  }

  def registerSimpleFeatureClasses(kryo: Kryo): Unit = {
    val serializer = new com.esotericsoftware.kryo.Serializer[SimpleFeature]() {
      val cache = new ConcurrentHashMap[Int, SimpleFeatureSerializer]()

      override def write(kryo: Kryo, out: Output, feature: SimpleFeature): Unit = {
        val id = GeoMesaSparkKryoRegistrator.putType(feature.getFeatureType)
        var serializer = cache.get(id)
        if (serializer == null) {
          serializer = new SimpleFeatureSerializer(feature.getFeatureType)
          cache.put(id, serializer)
        }
        out.writeInt(id, true)
        serializer.write(kryo, out, feature)
      }

      override def read(kryo: Kryo, in: Input, clazz: Class[SimpleFeature]): SimpleFeature = {
        val id = in.readInt(true)
        var serializer = cache.get(id)
        if (serializer == null) {
          serializer = new SimpleFeatureSerializer(GeoMesaSparkKryoRegistrator.getType(id))
          cache.put(id, serializer)
        }
        serializer.read(kryo, in, clazz)
      }
    }
    kryo.setReferences(false)
    GeoMesaSparkKryoRegistrator.SimpleFeatureImpls.foreach(kryo.register(_, serializer, kryo.getNextRegistrationId))
<<<<<<< HEAD
  }

  def registerSedonaClasses(kryo: Kryo): Unit = {
    val registratorClass = Try(Class.forName("org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator")).getOrElse(
      Class.forName("org.apache.sedona.core.serde.SedonaKryoRegistrator"))
    logger.debug(s"found sedona kryo registrator class ${registratorClass.getCanonicalName}")
    val sedonaRegistrator = registratorClass.newInstance().asInstanceOf[KryoRegistrator]
    sedonaRegistrator.registerClasses(kryo)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  }
}

object GeoMesaSparkKryoRegistrator {

  private val typeCache = new ConcurrentHashMap[Int, SimpleFeatureType]()

  private val SimpleFeatureImpls: Seq[Class[_ <: SimpleFeature]] =
    Seq(
      classOf[ScalaSimpleFeature],
      classOf[ImmutableSimpleFeature],
      classOf[LazyImmutableSimpleFeature],
      classOf[LazyMutableSimpleFeature],
      classOf[KryoBufferSimpleFeature],
      classOf[TransformSimpleFeature],
      classOf[SimpleFeatureImpl],
      classOf[SimpleFeature]
    )

  GeoMesaSparkKryoRegistratorEndpoint.init()

  def identifier(sft: SimpleFeatureType): Int = math.abs(MurmurHash3.stringHash(CacheKeyGenerator.cacheKey(sft)))

  def putType(sft: SimpleFeatureType): Int = {
    val id = identifier(sft)
    if (typeCache.putIfAbsent(id, sft) == null) {
      GeoMesaSparkKryoRegistratorEndpoint.Client.putType(id, sft)
    }
    id
  }

  def putTypes(types: Seq[SimpleFeatureType]): Seq[Int] =
    types.map(putType)

  def getType(id: Int): SimpleFeatureType =
    Option(typeCache.get(id)).orElse {
        fromSystemProperties(id).orElse(GeoMesaSparkKryoRegistratorEndpoint.Client.getType(id)).map {
          sft => typeCache.put(id, sft); sft
        }
      }.orNull

  def getTypes: Seq[SimpleFeatureType] = Seq(typeCache.values.asScala.toSeq: _*)

  def register(ds: DataStore): Unit = register(ds.getTypeNames.map(ds.getSchema))

  def register(sfts: Seq[SimpleFeatureType]): Unit = sfts.foreach(register)

  def register(sft: SimpleFeatureType): Unit = GeoMesaSparkKryoRegistrator.putType(sft)

  def systemProperties(schemas: SimpleFeatureType*): Seq[(String, String)] = {
    schemas.flatMap { sft =>
      val id = identifier(sft)
      val nameProp = (s"geomesa.types.$id.name", sft.getTypeName)
      val specProp = (s"geomesa.types.$id.spec", SimpleFeatureTypes.encodeType(sft))
      Seq(nameProp, specProp)
    }
  }

  private def fromSystemProperties(id: Int): Option[SimpleFeatureType] =
    for {
      name <- Option(GeoMesaSystemProperties.getProperty(s"geomesa.types.$id.name"))
      spec <- Option(GeoMesaSystemProperties.getProperty(s"geomesa.types.$id.spec"))
    } yield {
      SimpleFeatureTypes.createType(name, spec)
    }
}


