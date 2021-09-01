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
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, EmptyPlan}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory, AccumuloQueryPlan}
import org.locationtech.geomesa.accumulo.jobs.AccumuloJobUtils
import org.locationtech.geomesa.accumulo.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}

class AccumuloSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {

  import scala.collection.JavaConverters._

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    val paramsAsJava = params.asJava
    val ds = DataStoreFinder.getDataStore(paramsAsJava).asInstanceOf[AccumuloDataStore]

    lazy val transform = query.getHints.getTransformSchema

    def queryPlanToRDD(sft: SimpleFeatureType, qp: AccumuloQueryPlan): RDD[SimpleFeature] = {
      if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[SimpleFeature]
      } else {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
=======
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
        val config = new JobConf(conf)
=======
        val config = new Configuration(conf)
<<<<<<< HEAD
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
        val config = new JobConf(conf)
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 85d211ca2b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
>>>>>>> locationtech-main
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
        val config = new JobConf(conf)
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa627812da (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> ea0c690e23 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
=======
>>>>>>> locationtech-main
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d5 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
        val config = new JobConf(conf)
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> f5ea49f0c4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
        val config = new JobConf(conf)
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
>>>>>>> a8ce2896de (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> locationtech-main
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
        val config = new JobConf(conf)
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
=======
=======
>>>>>>> ad34cbf39d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
>>>>>>> 2671cb8522 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> locationtech-main
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> f8af85bf92 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
        val config = new JobConf(conf)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
        val config = new JobConf(conf)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> f5ea49f0c4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD:geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/accumulo/spark/AccumuloSpatialRDDProvider.scala
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
<<<<<<< HEAD
>>>>>>> ad34cbf39d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792)):geomesa-accumulo/geomesa-accumulo-spark/src/main/scala/org/locationtech/geomesa/spark/accumulo/AccumuloSpatialRDDProvider.scala
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
        GeoMesaAccumuloInputFormat.configure(config, paramsAsJava, qp, Some(ds.auths))
        sc.newAPIHadoopRDD(config, classOf[GeoMesaAccumuloInputFormat], classOf[Text], classOf[SimpleFeature]).map(_._2)
      }
    }

    try {
      // get the query plan to set up the iterators, ranges, etc
      // getMultipleQueryPlan will return the fallback if any
      // element of the plan is a JoinPlan
      val sft = ds.getSchema(query.getTypeName)
      val qps = AccumuloJobUtils.getMultipleQueryPlan(ds, query)

      // can return a union of the RDDs because the query planner *should*
      // be rewriting ORs to make them logically disjoint
      // e.g. "A OR B OR C" -> "A OR (B NOT A) OR ((C NOT A) NOT B)"
      val sfrdd = if (qps.lengthCompare(1) == 0 && qps.head.tables.lengthCompare(1) == 0) {
        queryPlanToRDD(sft, qps.head) // no union needed for single query plan
      } else {
        // flatten and duplicate the query plans so each one only has a single table
        val expanded = qps.flatMap {
          case qp: BatchScanPlan => qp.tables.map(t => qp.copy(tables = Seq(t)))
          case qp: EmptyPlan => Seq(qp)
          case qp => throw new NotImplementedError(s"Unexpected query plan type: $qp")
        }
        sc.union(expanded.map(queryPlanToRDD(sft, _)))
      }
      SpatialRDD(sfrdd, transform.getOrElse(sft))
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
    * @param rdd rdd
    * @param params params
    * @param typeName type name
    */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    WithStore[AccumuloDataStore](params) { ds =>
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save. Call createSchema on the DataStore first.")
    }

    rdd.foreachPartition { iter =>
      WithStore[AccumuloDataStore](params) { ds =>
        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          iter.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }
}
