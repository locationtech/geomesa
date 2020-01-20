/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStore, DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}
import org.opengis.feature.simple.SimpleFeature

/**
  * Spatial RDD provider for arbitrary geotools data stores. If available, prefer a GeoMesa-specific
  * implementation (e.g. HBaseSpatialRDDProvider), as it will be better optimized.
  *
  * To avoid being picked up inadvertently, the data store params must include "geotools" -> "true"
  */
class GeoToolsSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {

  import scala.collection.JavaConverters._

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean = {
    val parameters = params.asInstanceOf[java.util.Map[String, java.io.Serializable]]
    Option(params.get("geotools")).exists(FastConverter.convert(_, classOf[java.lang.Boolean])) &&
        DataStoreFinder.getAllDataStores.asScala.exists(_.canProcess(parameters))
  }

  override def rdd(
      conf: Configuration,
      sc: SparkContext,
      params: Map[String, String],
      query: Query): SpatialRDD = {
    WithStore[DataStore](params) { ds =>
      val (sft, features) = WithClose(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) { reader =>
        (reader.getFeatureType, CloseableIterator(reader).toList)
      }
      SpatialRDD(sc.parallelize(features), sft)
    }
  }

  /**
    * Writes this RDD to a GeoMesa data store. The type must exist in the data store, and all of the features
    * in the RDD must be of this type.
    *
    * @param rdd features to write
    * @param params data store params
    * @param typeName simple feature type name
    */
  override def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    WithStore[DataStore](params) { ds =>
      require(ds != null, "Could not load data store with the provided parameters")
      require(ds.getSchema(typeName) != null, "Schema must exist before calling save - use `DataStore.createSchema`")
    }

    rdd.foreachPartition { iter =>
      WithStore[DataStore](params) { ds =>
        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          iter.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }
}
