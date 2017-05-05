/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import java.io.File
import java.nio.file.Files

import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowDataStoreTest extends Specification {

  import scala.collection.JavaConversions._

  // note: need to compare as we iterate since values are only valid until 'next'
  def compare(features: Iterator[SimpleFeature], expected: Seq[SimpleFeature]): MatchResult[Any] = {
    var i = 0
    while (features.hasNext) {
      features.next mustEqual expected(i)
      i += 1
    }
    i mustEqual expected.length
  }

  "ArrowDataStore" should {
    "write and read values" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features0 = (0 until 10).map { i =>
        ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
      }
      val features1 = (10 until 20).map { i =>
        ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"2017-03-15T00:$i:00.000Z", s"POINT (4${i -10} 5${i -10})")
      }

      val file = Files.createTempFile("gm-arrow-ds", ".arrow").toUri.toURL
      try {
        val ds = DataStoreFinder.getDataStore(Map("url" -> file)).asInstanceOf[ArrowDataStore]
        ds must not(beNull)

        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        var writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        features0.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, overrideFid = true)
          writer.write()
        }
        writer.close()

        var results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))
        try {
          compare(results, features0)
        } finally {
          results.close()
        }

        writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        features1.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, overrideFid = true)
          writer.write()
        }
        writer.close()

        results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))
        try {
          compare(results, features0 ++ features1)
        } finally {
          results.close()
        }
      } finally {
        if (!new File(file.getPath).delete()) {
          new File(file.getPath).deleteOnExit()
        }
      }
    }
  }
}
