/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2022 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, EmptyPlan}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory, AccumuloQueryPlan}
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.mapreduce._
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c385f79943 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a358de35a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2b62bcaeb4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75f3f455ec (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8871b2b4bc (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4f922d74c9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0e67f36558 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89ea8190e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1230d64006 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5521d97d99 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> adf2ac19a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3171e7407d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> bd17bd131 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> c555446b8b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c385f7994 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
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
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1230d64006 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76328bb492 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 02ddf6ca41 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 7119f2c52a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c385f79943 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 5a7f445137 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 76328bb49 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4f922d74c9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 02ddf6ca4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a358de35a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d488f41800 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 9ce7ed900a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 2b62bcaeb4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75f3f455ec (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> ce07ef89fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 76328bb49 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 8871b2b4bc (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4f922d74c9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e67f36558 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 5e6674847 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5521d97d99 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 0e67f36558 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 7119f2c52 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> adf2ac19a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 89ea8190e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> f0b61142b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 0a6aff78fb (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d9cabf401c (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> bd17bd131 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> c555446b8b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 1230d64006 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> c385f7994 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3171e7407d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
        val config = new JobConf(conf)
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c385f79943 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a358de35a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 2b62bcaeb4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 75f3f455ec (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8871b2b4bc (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4f922d74c9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e67f36558 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 89ea8190e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1230d64006 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5521d97d99 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> adf2ac19a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3171e7407d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> bd17bd131 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> c555446b8b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c385f7994 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> locationtech-main
=======
        val config = new Configuration(conf)
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3baf74fa6 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a7f445137 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ce07ef89fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d9cabf401c (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 1230d64006 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> c385f79943 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 75f3f455ec (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> c385f7994 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3171e7407d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3171e7407d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 76328bb492 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ddf6ca41 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
        val config = new JobConf(conf)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7119f2c52a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> c385f79943 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5a7f445137 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 76328bb49 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 4f922d74c9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 02ddf6ca4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a358de35a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
        val config = new JobConf(conf)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d488f41800 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 9ce7ed900a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 2b62bcaeb4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 75f3f455ec (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> ce07ef89fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 76328bb49 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 8871b2b4bc (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 4f922d74c9 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b5719bd33 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> de3e57f03 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 92feb5d273 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 113b056da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6f73f22005 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 58f72e76c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8a234c7900 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 74c6b7f8d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 76328bb49 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 72415e208a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> c14e106409 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5e6674847 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5521d97d99 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 0e67f36558 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
        val config = new JobConf(conf)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 85d211ca2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 7119f2c52 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> adf2ac19a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 89ea8190e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 803762354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e62505d539 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3226e92d8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> f0b61142b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 0a6aff78fb (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> d9cabf401c (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> bd17bd131 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> c555446b8b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 1230d64006 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> ca22354dd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 797b02396b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> cd5564bac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> ea0c690e2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> c385f7994 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 3171e7407d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 038b4ca087 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
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
