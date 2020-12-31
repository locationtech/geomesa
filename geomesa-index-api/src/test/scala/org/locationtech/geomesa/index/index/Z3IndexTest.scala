/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z3IndexTest extends Specification with LazyLogging {

  "Z3Index" should {
    "index and query yearly epochs correctly" in {
      val spec =
        "name:String,track:String,dtg:Date,*geom:Point:srid=4326;" +
            "geomesa.z3.interval=year,geomesa.indices.enabled=z3:geom:dtg"

      val sft = SimpleFeatureTypes.createType("test", spec)

      val ds = new TestGeoMesaDataStore(false) // requires strict bbox...

      // note: 2020 was a leap year
      val features =
        (0 until 10).map { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track1", s"2020-12-07T0$i:00:00.000Z", s"POINT(4$i 60)")
        } ++ (10 until 20).map { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track2", s"2020-12-${i}T$i:00:00.000Z", s"POINT(4${i - 10} 60)")
        } ++ (20 until 30).map { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track3", s"2020-12-${i}T${i-10}:00:00.000Z", s"POINT(6${i - 20} 60)")
        } ++ (30 until 32).map { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track4", s"2020-12-${i}T${i-10}:00:00.000Z", s"POINT(${i - 20} 60)")
        }

      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
          writer.write()
        }
      }

      val filter = ECQL.toFilter("bbox(geom,0,55,70,65) AND dtg during 2020-12-01T00:00:00.000Z/2020-12-31T23:59:59.999Z")

      SelfClosingIterator(ds.getFeatureReader(new Query("test", filter), Transaction.AUTO_COMMIT)).toList must
          containTheSameElementsAs(features)
    }
  }
}
