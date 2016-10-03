/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.compute.spark

import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.EmptyPlan
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer
import org.locationtech.geomesa.features.{ScalaSimpleFeatureFactory, SimpleFeatureSerializers}
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaInputFormat
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobUtils}
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

object GeoMesaSpark extends LazyLogging {

  def init(conf: SparkConf, ds: DataStore): SparkConf = init(conf, ds.getTypeNames.map(ds.getSchema))

  def init(conf: SparkConf, sfts: Seq[SimpleFeatureType]): SparkConf = {
    import GeoMesaInputFormat.SYS_PROP_SPARK_LOAD_CP
    val typeOptions = GeoMesaSparkKryoRegistrator.systemProperties(sfts: _*)
    typeOptions.foreach { case (k,v) => System.setProperty(k, v) }
    val typeOpts = typeOptions.map { case (k,v) => s"-D$k=$v" }
    val jarOpt = sys.props.get(SYS_PROP_SPARK_LOAD_CP).map(v => s"-D$SYS_PROP_SPARK_LOAD_CP=$v")
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

  def register(ds: DataStore): Unit = register(ds.getTypeNames.map(ds.getSchema))

  def register(sfts: Seq[SimpleFeatureType]): Unit = sfts.foreach(GeoMesaSparkKryoRegistrator.putTypeIfAbsent)

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query,
          numberOfSplits: Option[Int]): RDD[SimpleFeature] = {
    rdd(conf, sc, dsParams, query, useMock = false, numberOfSplits)
  }

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query,
          useMock: Boolean = false,
          numberOfSplits: Option[Int] = None): RDD[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    try {
      // get the query plan to set up the iterators, ranges, etc
      lazy val sft = ds.getSchema(query.getTypeName)
      lazy val qp = JobUtils.getSingleQueryPlan(ds, query)

      if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[SimpleFeature]
      } else {
        val username = AccumuloDataStoreParams.userParam.lookUp(dsParams).toString
        val password = new PasswordToken(AccumuloDataStoreParams.passwordParam.lookUp(dsParams).toString.getBytes)
        val instance = ds.connector.getInstance().getInstanceName
        lazy val zookeepers = ds.connector.getInstance().getZooKeepers

        val transform = query.getHints.getTransformSchema

        ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, username, password)
        if (useMock){
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

        if (numberOfSplits.isDefined) {
          GeoMesaConfigurator.setDesiredSplits(conf, numberOfSplits.get * sc.getExecutorStorageStatus.length)
          InputConfigurator.setAutoAdjustRanges(classOf[AccumuloInputFormat], conf, false)
          InputConfigurator.setAutoAdjustRanges(classOf[GeoMesaInputFormat], conf, false)
        }
        GeoMesaConfigurator.setSerialization(conf)
        GeoMesaConfigurator.setTable(conf, qp.table)
        GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
        GeoMesaConfigurator.setFeatureType(conf, sft.getTypeName)
        if (query.getFilter != Filter.INCLUDE) {
          GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(query.getFilter))
        }
        transform.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

        // Configure Auths from DS
        val auths = Option(AccumuloDataStoreParams.authsParam.lookUp(dsParams).asInstanceOf[String])
        auths.foreach { a =>
          val authorizations = new Authorizations(a.split(","): _*)
          InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], conf, authorizations)
        }

        sc.newAPIHadoopRDD(conf, classOf[GeoMesaInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)
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
        "feature type must exist before calling save.  Call .createSchema on the DataStore before calling .save")
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
        featureWriter.close()
        ds.dispose()
      }
    }
  }

  def countByDay(conf: Configuration, sccc: SparkContext, dsParams: Map[String, String], query: Query, dateField: String = "dtg") = {
    val d = rdd(conf, sccc, dsParams, query)
    val dayAndFeature = d.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyyMMdd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property(dateField)
      iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
    }
    val groupedByDay = dayAndFeature.groupBy { case (date, _) => date }
    groupedByDay.map { case (date, iter) => (date, iter.size) }
  }

  def shallowJoin(sc: SparkContext, coveringSet: RDD[SimpleFeature], data: RDD[SimpleFeature], key: String): RDD[SimpleFeature] = {
    // Broadcast sfts to executors
    GeoMesaSparkKryoRegistrator.broadcast(sc, data)

    // Broadcast covering set to executors
    val broadcastedCover = sc.broadcast(coveringSet.collect)

    // Key data by cover name
    val keyedData = data.mapPartitions { iter =>
      import org.locationtech.geomesa.utils.geotools.Conversions._

      iter.flatMap { sf =>
        // Iterate over covers until a match is found
        val it = broadcastedCover.value.iterator
        var container: Option[String] = None

        while (it.hasNext) {
          val cover = it.next()
          // If the cover's polygon contains the feature,
          // or in the case of non-point geoms, if they intersect, set the container
          if (cover.geometry.intersects(sf.geometry)) {
            container = Some(cover.getAttribute(key).asInstanceOf[String])
          }
        }
        // return the found cover as the key
        if (container.isDefined) {
          Some(container.get, sf)
        } else {
          None
        }
      }
    }

    // Get the indices and types of the attributes that can be aggregated and send them to the partitions
    val countableTypes = Seq("Integer", "Long", "Double")
    val typeNames = data.first.getType.getTypes.toIndexedSeq.map{t => t.getBinding.getSimpleName.toString}

    val countableIndices = typeNames.indices.flatMap { index =>
      val featureType = typeNames(index)
      // Only grab countable types, skipping the ID field
      if ((countableTypes contains featureType) && index != 0) {
        Some(index, featureType)
      } else {
        None
      }
    }.toArray
    val countable = sc.broadcast(countableIndices)

    // Create a Simple Feature Type based on what can be aggregated
    val sftBuilder = new SftBuilder()
    sftBuilder.stringType(key)
    sftBuilder.multiPolygon("geom")
    sftBuilder.intType("count")
    val featureProperties = data.first.getProperties.toSeq
    countableIndices.foreach{ case (index, clazz) =>
        val featureName = featureProperties.apply(index).getName
        clazz match {
          case "Integer" => sftBuilder.intType(s"total_$featureName")
          case "Long" => sftBuilder.longType(s"total_$featureName")
          case "Double" => sftBuilder.doubleType(s"total_$featureName")
        }
        sftBuilder.doubleType(s"avg_${featureProperties.apply(index).getName}")
    }
    val coverSft = SimpleFeatureTypes.createType("aggregate", sftBuilder.getSpec)

    // Register it with kryo and send it to executors
    GeoMesaSpark.register(Seq(coverSft))
    GeoMesaSparkKryoRegistrator.broadcast(sc, keyedData)
    val coverSftBroadcast = sc.broadcast(SimpleFeatureTypes.encodeType(coverSft))

    // Pre-compute known indices and send them to workers
    val stringAttrs = coverSft.getAttributeDescriptors.map(_.getLocalName)
    val countIndex = sc.broadcast(stringAttrs.indexOf("count"))
    // Reduce features by their covering area
    val aggregate = reduceAndAggregate(keyedData, countable, countIndex, coverSftBroadcast)

    // Send a map of cover name -> geom to the executors
    import org.locationtech.geomesa.utils.geotools.Conversions._
    val coverMap: scala.collection.Map[String, Geometry] =
      coveringSet.map{ sf =>
        sf.getAttribute(key).asInstanceOf[String] -> sf.geometry
      }.collectAsMap

    val broadcastedCoverMap = sc.broadcast(coverMap)

    // Compute averages and set cover names and geometries
    aggregate.mapPartitions { iter =>
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

      iter.flatMap{ case (coverName, sf) =>
        if (sf.getType.getTypeName == "aggregate") {
          sf.getProperties.foreach{ prop =>
            val name = prop.getName.toString
            if (name.startsWith("total_")) {
              val count = sf.get[Integer]("count")
              val avg = prop.getValue match {
                case a: Integer => a.toDouble / count
                case a: java.lang.Long => a.toDouble / count
                case a: java.lang.Double => a / count
                case _ => throw new Exception(s"couldn't match $name")
              }
              sf.setAttribute(s"avg_${name.substring(6)}", avg)
            }
          }
          sf.setAttribute(key, coverName)
          sf.setDefaultGeometry(broadcastedCoverMap.value.getOrElse(coverName, null))
          Some(sf)
        } else {
          None
        }
      }
    }
  }

  def reduceAndAggregate(keyedData: RDD[(String, SimpleFeature)],
                         countable: Broadcast[Array[(Int, String)]],
                         countIndex: Broadcast[Int],
                         coverSftBroadcast: Broadcast[String]): RDD[(String, SimpleFeature)] = {

    // Reduce features by their covering area
    val aggregate = keyedData.reduceByKey((featureA, featureB) => {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

      val aggregateSft = SimpleFeatureTypes.createType("aggregate", coverSftBroadcast.value)

      val typeA = featureA.getType.getTypeName
      val typeB = featureB.getType.getTypeName
      // Case: combining two aggregate features
      if (typeA == "aggregate" && typeB == "aggregate") {
        // Combine the "total" properties
        (featureA.getProperties, featureB.getProperties).zipped.foreach((propA, propB) => {
          val name = propA.getName.toString
          if (propA.getName.toString.startsWith("total_") || propA.getName.toString == "count") {
            val sum = (propA.getValue, propB.getValue) match {
              case (a: Integer, b: Integer) => a + b
              case (a: java.lang.Long, b: java.lang.Long) => a + b
              case (a: java.lang.Double, b: java.lang.Double) => a + b
              case _ => throw new Exception("Couldn't match countable type.")
            }
            featureA.setAttribute(propA.getName, sum)
          }
        })
        featureA
      // Case: combining two regular features
      } else if (typeA != "aggregate" && typeB != "aggregate") {
        // Grab each feature's properties
        val featurePropertiesA = featureA.getProperties.toSeq
        val featurePropertiesB = featureB.getProperties.toSeq
        // Create a new aggregate feature to hold the result
        val featureFields = Seq("empty", featureA.geometry) ++ Seq.fill(aggregateSft.getTypes.size - 2)("0")
        val aggregateFeature = ScalaSimpleFeatureFactory.buildFeature(aggregateSft, featureFields, featureA.getID)

        // Loop over the countable properties and sum them for both geonames simple features
        countable.value.foreach { case (index, clazz) =>
          val propA = featurePropertiesA(index)
          val propB = featurePropertiesB(index)
          val valA = if (propA == null) 0 else propA.getValue
          val valB = if (propB == null) 0 else propB.getValue

          // Set the total
          if( propA != null && propB != null) {
            val sum  = (valA, valB) match {
              case (a: Integer, b: Integer) => a + b
              case (a: java.lang.Long, b: java.lang.Long) => a + b
              case (a: java.lang.Double, b: java.lang.Double) => a + b
              case x => throw new Exception(s"Couldn't match countable type. $x")
            }
            aggregateFeature.setAttribute(s"total_${propA.getName.toString}", sum)
          } else {
            val sum = if (valA != null) valA else if (valB != null) valB else 0
            aggregateFeature.setAttribute(s"total_${propB.getName.toString}", sum)
          }
        }
        aggregateFeature.setAttribute(countIndex.value, new Integer(2))
        aggregateFeature
      // Case: combining a mix
      } else {

        // Figure out which feature is which
        val (aggFeature: SimpleFeature, geoFeature: SimpleFeature) = if (typeA == "aggregate" && typeB != "aggregate") {
          (featureA, featureB)
        } else if (typeA != "aggregate" && typeB == "aggregate") {
          (featureB, featureA)
        }

        // Loop over the aggregate feature's properties, adding on the regular feature's properties
        aggFeature.getProperties.foreach { prop =>
          val name = prop.getName.toString
          if (name.startsWith("total_")) {
            val geoProp = geoFeature.getProperty(name.substring(6))
            if (geoProp != null) {
              val sum = (prop.getValue, geoProp.getValue) match {
                case (a: Integer, b: Integer) => a + b
                case (a: java.lang.Long, b: java.lang.Long) => a + b
                case (a: java.lang.Double, b: java.lang.Double) => a + b
                case _ => 0
              }
              aggFeature.setAttribute(name, sum)
            }
          }

        }
        aggFeature.setAttribute(countIndex.value, aggFeature.get[Integer](countIndex.value) + 1)
        aggFeature
      }
    })
    aggregate
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

  def broadcast(sc: SparkContext, partitions: RDD[_]): Unit = {
    import scala.collection.JavaConversions._

    val encodedTypes = typeCache.map { case (_, sft) => (sft.getTypeName, SimpleFeatureTypes.encodeType(sft)) }
    val broadcast = sc.broadcast(encodedTypes.toArray)
    partitions.foreachPartition { _ =>
      broadcast.value.foreach { case (name, spec) => putType(SimpleFeatureTypes.createType(name, spec)) }
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
      name <- sys.props.get(s"geomesa.types.$id.name")
      spec <- sys.props.get(s"geomesa.types.$id.spec")
    } yield {
      SimpleFeatureTypes.createType(name, spec)
    }
}
