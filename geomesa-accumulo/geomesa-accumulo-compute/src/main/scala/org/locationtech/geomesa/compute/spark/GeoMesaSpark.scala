/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.compute.spark

import java.util.concurrent.ConcurrentHashMap

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.EmptyPlan
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer
import org.locationtech.geomesa.features.SimpleFeatureSerializers
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.hashing.MurmurHash3

object GeoMesaSpark extends LazyLogging {

  def init(conf: SparkConf, ds: DataStore): SparkConf = init(conf, ds.getTypeNames.map(ds.getSchema))

  def init(conf: SparkConf, sfts: Seq[SimpleFeatureType]): SparkConf = {
    import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat.SYS_PROP_SPARK_LOAD_CP
    val typeOptions = GeoMesaSparkKryoRegistrator.systemProperties(sfts: _*)
    typeOptions.foreach { case (k,v) => System.setProperty(k, v) }
    val typeOpts = typeOptions.map { case (k,v) => s"-D$k=$v" }
    val jarOpt = Option(GeoMesaSystemProperties.getProperty(SYS_PROP_SPARK_LOAD_CP)).map(v => s"-D$SYS_PROP_SPARK_LOAD_CP=$v")
    val extraOpts = (typeOpts ++ jarOpt).mkString(" ")
    val newOpts = if (conf.contains("spark.executor.extraJavaOptions")) {
      conf.get("spark.executor.extraJavaOptions").concat(" ").concat(extraOpts)
    } else {
      extraOpts
    }

    conf.set("spark.executor.extraJavaOptions", newOpts)
    // These configurations can be set in spark-defaults.conf
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
  }

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query): RDD[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    val username = AccumuloDataStoreParams.userParam.lookUp(dsParams).toString
    val password = new PasswordToken(AccumuloDataStoreParams.passwordParam.lookUp(dsParams).toString.getBytes)
    try {
      // get the query plan to set up the iterators, ranges, etc
      lazy val sft = ds.getSchema(query.getTypeName)
      lazy val qp = AccumuloJobUtils.getSingleQueryPlan(ds, query)

      if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[SimpleFeature]
      } else {
        val instance = ds.connector.getInstance().getInstanceName
        val zookeepers = ds.connector.getInstance().getZooKeepers

        val transform = query.getHints.getTransformSchema

        ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, username, password)
        if (Try(dsParams("useMock").toBoolean).getOrElse(false)){
          ConfiguratorBase.setMockInstance(classOf[AccumuloInputFormat], conf, instance)
        } else {
          ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf, instance, zookeepers)
        }
        InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, qp.table)
        InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, qp.ranges)
        qp.iterators.foreach(InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, _))

        if (qp.columnFamilies.nonEmpty) {
          val cf = qp.columnFamilies.map(cf => new AccPair[Text, Text](cf, null))
          InputConfigurator.fetchColumns(classOf[AccumuloInputFormat], conf, cf)
        }

        InputConfigurator.setBatchScan(classOf[AccumuloInputFormat], conf, true)
        InputConfigurator.setBatchScan(classOf[GeoMesaAccumuloInputFormat], conf, true)
        GeoMesaConfigurator.setSerialization(conf)
        GeoMesaConfigurator.setTable(conf, qp.table)
        GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
        GeoMesaConfigurator.setFeatureType(conf, sft.getTypeName)

        // set the secondary filter if it exists and is  not Filter.INCLUDE
        qp.filter.secondary
          .collect { case f if f != Filter.INCLUDE => f }
          .foreach { f => GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(f)) }

        transform.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

        // Configure Auths from DS
        val auths = Option(AccumuloDataStoreParams.authsParam.lookUp(dsParams).asInstanceOf[String])
        auths.foreach { a =>
          val authorizations = new Authorizations(a.split(","): _*)
          InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], conf, authorizations)
        }

        sc.newAPIHadoopRDD(conf, classOf[GeoMesaAccumuloInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)
      }
    } finally {
      if (ds != null) {
        ds.dispose()
      }
    }
  }

  /**
   * Writes this RDD to a GeoMesa table.
   * The type must exist in the data store, and all of the features in the RDD must be of this type.
   *
   * @param rdd
   * @param writeDataStoreParams
   * @param writeTypeName
   */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[AccumuloDataStore]
    try {
      require(ds.getSchema(writeTypeName) != null,
        "Feature type must exist before calling save.  Call .createSchema on the DataStore before calling .save")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[AccumuloDataStore]
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)
      val attrNames = featureWriter.getFeatureType.getAttributeDescriptors.map(_.getLocalName)
      try {
        iter.foreach { case rawFeature =>
          val newFeature = featureWriter.next()
          attrNames.foreach(an => newFeature.setAttribute(an, rawFeature.getAttribute(an)))
          featureWriter.write()
        }
      } finally {
        IOUtils.closeQuietly(featureWriter)
        ds.dispose()
      }
    }
  }
}

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
