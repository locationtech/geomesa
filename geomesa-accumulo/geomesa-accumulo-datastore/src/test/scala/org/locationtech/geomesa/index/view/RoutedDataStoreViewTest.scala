/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.feature.NameImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.jts.geom.Point
import org.specs2.runner.JUnitRunner

import java.nio.file.{Files, Path}
import java.util.Date

@RunWith(classOf[JUnitRunner])
class RoutedDataStoreViewTest extends TestWithFeatureType {

  import scala.collection.JavaConverters._

  // note: shp seems to just use an incrementing number for the feature id,
  // and return `<typeName>.` when returning feature them
  // as such, we don't compare primary keys directly here

  override val spec = "*the_geom:Point:srid=4326,name:String,age:Int,dtg:Date"

  lazy val features = Seq.tabulate(9) { u =>
    val i = u + 1 // sync up with shp fid
    // shp seems to round dates to whole days??
    ScalaSimpleFeature.create(sft, s"$i", s"POINT (45 5$i)", s"name$i", 20 + i, s"2018-01-0${i}T00:00:00.000Z")
  }

  lazy val accumuloParams =
    (dsParams +
      (RouteSelectorByAttribute.RouteAttributes -> Seq(Seq.empty.asJava, "id", "the_geom", Seq("dtg", "the_geom").asJava).asJava)).asJava

  var shpParams: java.util.Map[String, _] = _

  var path: Path = _
  var routedDs: RoutedDataStoreView = _

  def comboParams(params: java.util.Map[String, _]*): java.util.Map[String, String] = {
    val configs = params.map(ConfigValueFactory.fromMap).asJava
    val config = ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(configs))
    Map(RoutedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())).asJava
  }

  step {
    addFeatures(features)
    SelfClosingIterator(ds.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList must
        haveLength(features.length)

    path = Files.createTempDirectory(s"route-ds-test")

    shpParams = Map(
      "url"                                    -> s"file://${path.toFile.getAbsolutePath}/$sftName.shp",
      RouteSelectorByAttribute.RouteAttributes -> Seq("name", "age").asJava
    ).asJava

    val shpDs = DataStoreFinder.getDataStore(shpParams)
    try {
      shpDs.createSchema(sft)
      WithClose(shpDs.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
        features.iterator.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
      SelfClosingIterator(shpDs.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList must
          haveLength(features.length)
    } finally {
      if (shpDs != null) {
        shpDs.dispose()
      }
    }

    routedDs = DataStoreFinder.getDataStore(comboParams(shpParams, accumuloParams)).asInstanceOf[RoutedDataStoreView]
    routedDs must not(beNull)
  }

  "MergedDataStoreView" should {
    "load multiple datastores" in {
      routedDs.getTypeNames mustEqual Array(sftName)
      routedDs.getNames.asScala mustEqual Seq(new NameImpl(sftName))

      val sft = routedDs.getSchema(sftName)

      sft.getAttributeCount mustEqual 4
      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("the_geom", "name", "age", "dtg")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Point], classOf[String], classOf[Integer], classOf[Date])
    }

    "query multiple data stores" in {
      val results = SelfClosingIterator(routedDs.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList

      results must haveLength(features.length)
      foreach(results.sortBy(_.getAttribute("name").asInstanceOf[String]).zip(features)) { case (actual, expected) =>
        // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
        expected.getAttributes.asScala mustEqual actual.getAttributes.asScala
      }
    }

    "query multiple data stores with filters and transforms" in {
      val filters = Seq(
        "IN('3', '4', '5', '6')",
        "bbox(the_geom,44,52.5,46,56.5)",
        "bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-02T12:00:00.000Z/2018-01-06T12:00:00.000Z",
        "name IN('name3', 'name4', 'name5', 'name6')"
      )
      val transforms = Seq(null, Array("the_geom"), Array("the_geom", "dtg"), Array("name"), Array("dtg", "the_geom", "age", "name"))

      foreach(filters) { filter =>
        val ecql = ECQL.toFilter(filter)
        foreach(transforms) { transform =>
          val query = new Query(sftName, ecql, transform: _*)
          val results = SelfClosingIterator(routedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          results must haveLength(4)
          val attributes = Option(transform).getOrElse(sft.getAttributeDescriptors.asScala.map(_.getLocalName).toArray)
          forall(results) { feature =>
            feature.getAttributes must haveLength(attributes.length)
            forall(attributes.zipWithIndex) { case (attribute, i) =>
              feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
              // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
              features.find(f => feature.getID.contains(f.getID)).get.getAttribute(attribute) mustEqual
                  feature.getAttribute(attribute)
            }
          }
        }
      }
    }
  }

  step {
    routedDs.dispose()
    PathUtils.deleteRecursively(path)
  }
}


