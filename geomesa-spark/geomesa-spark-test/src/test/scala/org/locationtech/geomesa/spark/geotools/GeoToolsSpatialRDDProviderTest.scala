/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.geotools

import org.apache.hadoop.conf.Configuration
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.geotools.spark.GeoToolsSpatialRDDProvider
import org.locationtech.geomesa.spark.{GeoMesaSpark, TestWithGeoToolsSpark}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose

class GeoToolsSpatialRDDProviderTest extends TestWithGeoToolsSpark {

  import scala.collection.JavaConverters._

  lazy val chicagoSft =
    SimpleFeatureTypes.createType("chicago",
      "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")

  lazy val chicagoFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(chicagoSft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
    ScalaSimpleFeature.create(chicagoSft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
    ScalaSimpleFeature.create(chicagoSft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)"),
    ScalaSimpleFeature.create(chicagoSft, "4", "true", 4, "2016-01-01T00:00:00.000Z", "POINT (-73.5 39.5)"),
    ScalaSimpleFeature.create(chicagoSft, "5", "true", 5, "2016-01-02T00:00:00.000Z", "POINT (-74.0 35.5)"),
    ScalaSimpleFeature.create(chicagoSft, "6", "true", 6, "2016-01-03T00:00:00.000Z", "POINT (-79.0 37.5)")
  )

  "The GeoToolsSpatialRDDProvider" should {
    "read from a database" in {
      val sft = SimpleFeatureTypes.renameSft(chicagoSft, "chicago_read")
      WithClose(DataStoreFinder.getDataStore(postgisParams.asJava)) { ds =>
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          chicagoFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        val rdd = GeoMesaSpark(postgisParams.asJava).rdd(new Configuration(), sparkContext, postgisParams, new Query(sft.getTypeName))
        rdd.count() mustEqual 3L
      }
    }

    "write to a database" in {
      val sft = SimpleFeatureTypes.renameSft(chicagoSft, "chicago_write")
      WithClose(DataStoreFinder.getDataStore(postgisParams.asJava)) { ds =>
        ds.createSchema(sft)
        val writeRdd = sparkContext.parallelize(chicagoFeatures)
        // need to bypass the store check up fron b/c ds params are different from the local check and the distributed write
        GeoToolsSpatialRDDProvider.StoreCheck.threadLocalValue.set("false")
        try {
          GeoMesaSpark(postgisParams.asJava).save(writeRdd, postgisSparkParams, sft.getTypeName)
        } finally {
          GeoToolsSpatialRDDProvider.StoreCheck.threadLocalValue.remove()
        }
        // verify write
        val readRdd = GeoMesaSpark(postgisParams.asJava).rdd(new Configuration(), sparkContext, postgisParams, new Query(sft.getTypeName))
        readRdd.count() mustEqual 6L
      }
    }
  }
}
