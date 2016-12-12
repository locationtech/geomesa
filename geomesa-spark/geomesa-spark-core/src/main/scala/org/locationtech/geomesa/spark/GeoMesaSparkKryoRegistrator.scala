package org.locationtech.geomesa.spark

import java.util.concurrent.ConcurrentHashMap

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.geotools.data.DataStore
import org.locationtech.geomesa.features.SimpleFeatureSerializers
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

class GeoMesaSparkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    val serializer = new com.esotericsoftware.kryo.Serializer[SimpleFeature]() {
      val cache = new ConcurrentHashMap[Int, SimpleFeatureSerializer]()

      override def write(kryo: Kryo, out: Output, feature: SimpleFeature): Unit = {
        val id = GeoMesaSparkKryoRegistrator.putTypeIfAbsent(feature.getFeatureType)
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
    SimpleFeatureSerializers.simpleFeatureImpls.foreach(kryo.register(_, serializer, kryo.getNextRegistrationId))
  }
}

object GeoMesaSparkKryoRegistrator {

  private val typeCache = new ConcurrentHashMap[Int, SimpleFeatureType]()

  def identifier(sft: SimpleFeatureType): Int = math.abs(MurmurHash3.stringHash(CacheKeyGenerator.cacheKey(sft)))

  def putType(sft: SimpleFeatureType): Int = {
    val id = identifier(sft)
    typeCache.put(id, sft)
    id
  }

  def putTypeIfAbsent(sft: SimpleFeatureType): Int = {
    val id = identifier(sft)
    typeCache.putIfAbsent(id, sft)
    id
  }

  def getType(id: Int): SimpleFeatureType =
    Option(typeCache.get(id)).orElse {
      val fromProps = fromSystemProperties(id)
      fromProps.foreach(sft => typeCache.put(id, sft))
      fromProps
    }.orNull

  def register(ds: DataStore): Unit = register(ds.getTypeNames.map(ds.getSchema))

  def register(sfts: Seq[SimpleFeatureType]): Unit = sfts.foreach(GeoMesaSparkKryoRegistrator.putTypeIfAbsent)

  def broadcast(partitions: RDD[_]): Unit = {
    val encodedTypes = typeCache
      .map { case (_, sft) => (sft.getTypeName, SimpleFeatureTypes.encodeType(sft)) }
      .toArray
    partitions.foreachPartition { _ =>
      encodedTypes.foreach { case (name, spec) => putType(SimpleFeatureTypes.createType(name, spec)) }
    }
  }

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
