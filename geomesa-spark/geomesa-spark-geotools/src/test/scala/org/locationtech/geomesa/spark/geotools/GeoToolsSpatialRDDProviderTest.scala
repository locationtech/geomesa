/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.geotools

import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.compute.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLDataTest extends Specification {

  //      import scala.collection.JavaConversions._
  //      dsParams = Map("cqengine" -> "true", "geotools" -> "true")
  //      DataStoreFinder.getAvailableDataStores.foreach{println}


  ds = DataStoreFinder.getDataStore(dsParams)




}