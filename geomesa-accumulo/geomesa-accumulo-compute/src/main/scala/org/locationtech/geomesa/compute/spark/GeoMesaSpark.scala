/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.compute.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data._
import org.locationtech.geomesa.spark.accumulo.AccumuloSpatialRDDProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

@deprecated
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

  def rdd(conf: Configuration, sc: SparkContext, params: Map[String, String], query: Query): RDD[SimpleFeature] =
    new AccumuloSpatialRDDProvider().rdd(conf, sc, params, query)

  /**
   * Writes this RDD to a GeoMesa table.
   * The type must exist in the data store, and all of the features in the RDD must be of this type.
   *
   * @param rdd
   * @param params
   * @param typeName
   */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit =
    new AccumuloSpatialRDDProvider().save(rdd, params, typeName)
}
