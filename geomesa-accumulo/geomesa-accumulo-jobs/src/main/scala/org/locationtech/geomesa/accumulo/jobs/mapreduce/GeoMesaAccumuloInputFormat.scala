/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2024 Dstl
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2023 Dstl
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2022 Dstl
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
package org.locationtech.geomesa.accumulo.jobs.mapreduce
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
package org.locationtech.geomesa.jobs.mapreduce
<<<<<<< HEAD
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
package org.locationtech.geomesa.jobs.mapreduce
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
package org.locationtech.geomesa.jobs.mapreduce
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting.Column
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat
import org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloMapperProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloClientConfig, AccumuloDataStore, AccumuloDataStoreParams, AccumuloQueryPlan}
import org.locationtech.geomesa.accumulo.jobs.AccumuloJobUtils
import org.locationtech.geomesa.accumulo.jobs.mapreduce.GeoMesaAccumuloInputFormat.{GeoMesaRecordReader, GroupedSplit}
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.WithStore

import java.io._
import java.net.{URL, URLClassLoader}
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
import java.util.AbstractMap.SimpleImmutableEntry
import java.util.Map.Entry
import java.util.{Collections, Properties}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
import java.nio.charset.StandardCharsets
import java.util.AbstractMap.SimpleImmutableEntry
import java.util.Collections
import java.util.Map.Entry
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
import scala.collection.mutable.ArrayBuffer

/**
  * Input format that allows processing of simple features from GeoMesa based on a CQL query
  */
class GeoMesaAccumuloInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging {

  import scala.collection.JavaConverters._

  private val delegate = new AccumuloInputFormat()

  /**
    * Gets splits for a job.
    *
    * Our delegated AccumuloInputFormat creates a split for each range - because we set a lot of ranges in
    * geomesa, that creates too many mappers. Instead, we try to group the ranges by tservers. We use the
    * location assignment of the tablets to tservers to determine the number of splits returned.
    */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val accumuloSplits = delegate.getSplits(context)
    // Get the appropriate number of mapper splits using the following priority
    // 1. Get splits from AccumuloMapperProperties.DESIRED_ABSOLUTE_SPLITS (geomesa.mapreduce.splits.max)
    // 2. Get splits from #tserver locations * AccumuloMapperProperties.DESIRED_SPLITS_PER_TSERVER (geomesa.mapreduce.splits.tserver.max)
    // 3. Get splits from AccumuloInputFormat.getSplits(context)
    def positive(prop: SystemProperty): Option[Int] = {
      val int = prop.toInt
      if (int.exists(_ < 1)) {
        throw new IllegalArgumentException(s"${prop.property} contains an invalid int: ${prop.get}")
      }
      int
    }

    val grpSplitsMax = positive(AccumuloMapperProperties.DESIRED_ABSOLUTE_SPLITS)

    lazy val grpSplitsPerTServer = positive(AccumuloMapperProperties.DESIRED_SPLITS_PER_TSERVER).flatMap { perTS =>
      val numLocations = accumuloSplits.asScala.flatMap(_.getLocations).distinct.length
      if (numLocations < 1) { None } else { Some(numLocations * perTS) }
    }

    grpSplitsMax.orElse(grpSplitsPerTServer) match {
      case Some(numberOfSplits) =>
        logger.debug(s"Using desired splits with result of $numberOfSplits splits")
        val splitSize: Int = math.ceil(accumuloSplits.size().toDouble / numberOfSplits).toInt
        accumuloSplits.asScala.groupBy(_.getLocations.head).flatMap { case (location, splits) =>
          splits.grouped(splitSize).map { group =>
            val split = new GroupedSplit()
            split.location = location
            split.splits.append(group.map(f => f.asInstanceOf[RangeInputSplit]).toSeq: _*)
            split.asInstanceOf[InputSplit]
          }
        }.toList.asJava

      case None =>
        logger.debug(s"Using default Accumulo Splits with ${accumuloSplits.size} splits")
        accumuloSplits
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): GeoMesaRecordReader = {
    val toFeatures = GeoMesaConfigurator.getResultsToFeatures[Entry[Key, Value]](context.getConfiguration)
    new GeoMesaRecordReader(toFeatures, delegate.createRecordReader(split, context))
  }
}

object GeoMesaAccumuloInputFormat extends LazyLogging {

  import scala.collection.JavaConverters._

  val SYS_PROP_SPARK_LOAD_CP = "org.locationtech.geomesa.spark.load-classpath"

  /**
   * Configure the input format based on a query
   *
   * @param conf configuration to update
   * @param params data store parameters
   * @param query query
   */
  def configure(conf: Configuration, params: java.util.Map[String, _], query: Query): Unit = {
    // get the query plan to set up the iterators, ranges, etc
    val plan = WithStore[AccumuloDataStore](params) { ds =>
      require(ds != null, "Invalid data store parameters")
      AccumuloJobUtils.getSingleQueryPlan(ds, query)
    }
    configure(conf, params, plan)
  }

  /**
   * Configure the input format based on a query plan
   *
   * @param conf configuration to update
   * @param params data store parameters
   * @param plan query plan
   */
  def configure(conf: Configuration, params: java.util.Map[String, _], plan: AccumuloQueryPlan): Unit = {
    val auths = AccumuloDataStoreParams.AuthsParam.lookupOpt(params).map(a => new Authorizations(a.split(","): _*))
    configure(conf, params, plan, auths)
  }

  /**
   * Configure the input format based on a query plan
   *
   * @param conf configuration to update
   * @param params data store parameters
   * @param plan query plan
   */
  def configure(
      conf: Configuration,
      params: java.util.Map[String, _],
      plan: AccumuloQueryPlan,
      auths: Option[Authorizations]): Unit = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 12aaaa71e3 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 868625d94 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 2c7e2525a7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 85d211ca2b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
>>>>>>> 0d3cbc99a2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
>>>>>>> aa627812da (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ea0c690e23 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d5 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> a8ce2896de (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> c4bc2d77a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 2671cb852 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> d10896ab3a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 2c7e2525a7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1656c7d3db (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d46c2b8e27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 2671cb8522 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> f8af85bf92 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> f5ea49f0c4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> ad34cbf39d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> d03ef04256 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b1c30e3f2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 6df49532c0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 6120534782 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> a8ce2896d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 01843b1112 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 12aaaa71e3 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c4bc2d77a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 89f8ab1b27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d10896ab3a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 868625d94 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 3e43fdb344 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 2c7e2525a7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 2671cb852 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d46c2b8e27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 1656c7d3db (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> f8af85bf9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 0e7f3a70c7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> cd2e14d63a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 70f1ed62e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 191f2f09a5 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
=======
    // assertion: all accumulo input config requires a job, but really just updates the job conf
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6120534782 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 12aaaa71e3 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> a8ce2896d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 2c7e2525a7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 85d211ca2b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ea0c690e23 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ad34cbf39d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> locationtech-main
=======
    // assertion: all accumulo input config requires a job, but really just updates the job conf
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812da (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> ea0c690e23 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
>>>>>>> locationtech-main
=======
    // assertion: all accumulo input config requires a job, but really just updates the job conf
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d5 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> a8ce2896de (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> f5ea49f0c4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 12aaaa71e3 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> d46c2b8e27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 2671cb852 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> a8ce2896de (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d46c2b8e27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 2671cb852 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d46c2b8e27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ad34cbf39d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 2671cb852 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d46c2b8e27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 1656c7d3db (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> locationtech-main
=======
    // assertion: all accumulo input config requires a job, but really just updates the job conf
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 2c7e2525a7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ad34cbf39d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2671cb8522 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1656c7d3db (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e7f3a70c7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f8af85bf9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f8af85bf9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> cd2e14d63a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> d03ef04256 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> locationtech-main
=======
    // assertion: all accumulo input config requires a job, but really just updates the job conf
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f8af85bf92 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> f5ea49f0c4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> locationtech-main
=======
    // assertion: all accumulo input config requires a job, but really just updates the job conf
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ad34cbf39d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d03ef04256 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> cd2e14d63a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> ad8d1be45 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7b3160b90a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ab33423d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> f0e859ddba (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> a1362333c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3109199f02 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> b1c30e3f2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 6df49532c0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 6120534782 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> a8ce2896d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 01843b1112 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 12aaaa71e3 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
<<<<<<< HEAD
>>>>>>> f9397984e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a2c63a57a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f345c93e8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
    // assertion: all accumulo input config requires a job, but really just updates the job conf
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> c4bc2d77a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 89f8ab1b27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d10896ab3a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 868625d94 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 3e43fdb344 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 2c7e2525a7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 7ea07a654 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0f2a67fb44 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6d73922328 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 2671cb852 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d46c2b8e27 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 1656c7d3db (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> eb01a83da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2005590fb6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0b213b35a8 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> f8af85bf9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 0e7f3a70c7 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> cd2e14d63a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
    // all accumulo input config methods requires a job
    // assertion: only the JobConf is updated - to get credentials pass in a JobConf instead of Configuration
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1dae86c846 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 70f1ed62e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 191f2f09a5 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
    val job = new Job(conf)
    job.setInputFormatClass(classOf[GeoMesaAccumuloInputFormat])

    val props = new Properties()
    // set zookeeper instance
    props.put(ClientProperty.INSTANCE_NAME.getKey, AccumuloDataStoreParams.InstanceNameParam.lookup(params))
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey, AccumuloDataStoreParams.ZookeepersParam.lookup(params))
    // set connector info
    val password = AccumuloDataStoreParams.PasswordParam.lookupOpt(params)
    password.orElse(AccumuloDataStoreParams.KeytabPathParam.lookupOpt(params)).foreach { token =>
      props.put(ClientProperty.AUTH_PRINCIPAL.getKey, AccumuloDataStoreParams.UserParam.lookup(params))
      props.put(ClientProperty.AUTH_TOKEN.getKey, token)
      if (password.isDefined) {
        props.put(ClientProperty.AUTH_TYPE.getKey, AccumuloClientConfig.PasswordAuthType)
      } else {
        props.put(ClientProperty.AUTH_TYPE.getKey, AccumuloClientConfig.KerberosAuthType)
        props.put(ClientProperty.SASL_ENABLED.getKey, "true")
      }
    }

<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/accumulo/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
        // TODO verify kerberos still works
//    val token = AccumuloDataStoreParams.PasswordParam.lookupOpt(params) match {
//      case Some(p) => new PasswordToken(p.getBytes(StandardCharsets.UTF_8))
//      case None =>
//        // must be using Kerberos
//        val file = new java.io.File(keytabPath)
//        // mimic behavior from accumulo 1.9 and earlier:
//        // `public KerberosToken(String principal, File keytab, boolean replaceCurrentUser)`
//        UserGroupInformation.loginUserFromKeytab(user, file.getAbsolutePath)
//        new KerberosToken(user, file)
//    }
//    // note: for Kerberos, this will create a DelegationToken for us and add it to the Job credentials
//    AbstractInputFormat.setConnectorInfo(job, user, token)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
    // note: for Kerberos, this will create a DelegationToken for us and add it to the Job credentials
    AbstractInputFormat.setConnectorInfo(job, user, token)

    auths.foreach(AbstractInputFormat.setScanAuthorizations(job, _))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-jobs/src/main/scala/org/locationtech/geomesa/jobs/mapreduce/GeoMesaAccumuloInputFormat.scala
>>>>>>> 15b11b70cf (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))

    // use the query plan to set the accumulo input format options
    require(plan.tables.lengthCompare(1) == 0, s"Can only query from a single table: ${plan.tables.mkString(", ")}")

    val builder = AccumuloInputFormat.configure().clientProperties(props).table(plan.tables.head).batchScan(true)
    auths.foreach(builder.auths)
    if (plan.ranges.nonEmpty) {
      builder.ranges(plan.ranges.asJava)
    }
    plan.columnFamily.foreach { colFamily =>
      builder.fetchColumns(Collections.singletonList(new Column(colFamily)))
    }
    plan.iterators.foreach(builder.addIterator)
    builder.store(job)

    // add the configurations back into the original conf
    conf.addResource(job.getConfiguration)

    GeoMesaConfigurator.setResultsToFeatures(conf, plan.resultsToFeatures)
    plan.reducer.foreach(GeoMesaConfigurator.setReducer(conf, _))
    plan.sort.foreach(GeoMesaConfigurator.setSorting(conf, _))
    plan.projection.foreach(GeoMesaConfigurator.setProjection(conf, _))
  }

  /**
   * This takes any jars that have been loaded by spark in the context classloader and makes them
   * available to the general classloader. This is required as not all classes (even spark ones) check
   * the context classloader.
   */
  def ensureSparkClasspath(): Unit = {
    val sysLoader = ClassLoader.getSystemClassLoader
    val ccl = Thread.currentThread().getContextClassLoader
    if (ccl == null || !ccl.getClass.getCanonicalName.startsWith("org.apache.spark.")) {
      logger.debug("No spark context classloader found")
    } else if (!ccl.isInstanceOf[URLClassLoader]) {
      logger.warn(s"Found context classloader, but can't handle type ${ccl.getClass.getCanonicalName}")
    } else if (!sysLoader.isInstanceOf[URLClassLoader]) {
      logger.warn(s"Found context classloader, but can't add to type ${sysLoader.getClass.getCanonicalName}")
    } else {
      // hack to get around protected visibility of addURL
      // this might fail if there is a security manager present
      val addUrl = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
      addUrl.setAccessible(true)
      val sysUrls = sysLoader.asInstanceOf[URLClassLoader].getURLs.map(_.toString).toSet
      val (dupeUrls, newUrls) = ccl.asInstanceOf[URLClassLoader].getURLs.filterNot(_.toString.contains("__app__.jar")).partition(url => sysUrls.contains(url.toString))
      newUrls.foreach(addUrl.invoke(sysLoader, _))
      logger.debug(s"Loaded ${newUrls.length} urls from context classloader into system classloader " +
          s"and ignored ${dupeUrls.length} that are already loaded")
    }
  }

  /**
    * Record reader that delegates to accumulo record readers and transforms the key/values coming back into
    * simple features.
    *
    * @param toFeatures results to features
    * @param reader delegate reader
    */
  class GeoMesaRecordReader(toFeatures: ResultsToFeatures[Entry[Key, Value]], reader: RecordReader[Key, Value])
      extends RecordReader[Text, SimpleFeature] {

    private val key = new Text()

    private var currentFeature: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
      reader.initialize(split, context)

    override def getProgress: Float = reader.getProgress

    override def nextKeyValue(): Boolean = {
      if (reader.nextKeyValue()) {
        currentFeature = toFeatures.apply(new SimpleImmutableEntry(reader.getCurrentKey, reader.getCurrentValue))
        key.set(currentFeature.getID)
        true
      } else {
        false
      }
    }

    override def getCurrentKey: Text = key

    override def getCurrentValue: SimpleFeature = currentFeature

    override def close(): Unit = reader.close()
  }

  /**
    * Input split that groups a series of RangeInputSplits. Has to implement Hadoop Writable, thus the vars and
    * mutable state.
    */
  class GroupedSplit extends InputSplit with Writable {

    // if we're running in spark, we need to load the context classpath before anything else,
    // otherwise we get classloading and serialization issues
    if (sys.env.get(GeoMesaAccumuloInputFormat.SYS_PROP_SPARK_LOAD_CP).exists(_.toBoolean)) {
      GeoMesaAccumuloInputFormat.ensureSparkClasspath()
    }

    private [mapreduce] var location: String = _
    private [mapreduce] val splits: ArrayBuffer[RangeInputSplit] = ArrayBuffer.empty

    override def getLength: Long = splits.foldLeft(0L)((l: Long, r: RangeInputSplit) => l + r.getLength)

    override def getLocations: Array[String] = if (location == null) { Array.empty } else { Array(location) }

    override def write(out: DataOutput): Unit = {
      out.writeUTF(location)
      out.writeInt(splits.length)
      splits.foreach(_.write(out))
    }

    override def readFields(in: DataInput): Unit = {
      location = in.readUTF()
      splits.clear()
      var i = 0
      val size = in.readInt()
      while (i < size) {
        val split = new RangeInputSplit()
        split.readFields(in)
        splits.append(split)
        i = i + 1
      }
    }

    override def toString = s"mapreduce.GroupedSplit[$location](${splits.length})"
  }
}
