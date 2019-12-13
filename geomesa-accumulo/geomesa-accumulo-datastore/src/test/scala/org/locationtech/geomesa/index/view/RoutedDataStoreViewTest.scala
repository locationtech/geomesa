/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.nio.file.{Files, Path}
import java.util.Date

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.feature.NameImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.jts.geom.Point
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RoutedDataStoreViewTest extends Specification {

  import scala.collection.JavaConverters._

  // note: h2 seems to require ints as the primary key, and then prepends `<typeName>.` when returning them
  // as such, we don't compare primary keys directly here
  // there may be a way to override this behavior but I haven't found it...

  sequential // note: shouldn't need to be sequential, but h2 doesn't do well with concurrent requests

  // we use class name to prevent spillage between unit tests in the mock connector
  val sftName: String = getClass.getSimpleName
  val spec = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType(sftName, spec)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", 20 + i, s"2018-01-01T00:0$i:00.000Z", s"POINT (45 5$i)")
  }

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val accumuloParams = Map(
    AccumuloDataStoreParams.InstanceIdParam.key -> "mycloud",
    AccumuloDataStoreParams.ZookeepersParam.key -> "myzoo",
    AccumuloDataStoreParams.UserParam.key       -> "user",
    AccumuloDataStoreParams.PasswordParam.key   -> "password",
    AccumuloDataStoreParams.CatalogParam.key    -> sftName,
    AccumuloDataStoreParams.MockParam.key       -> "true",
    RouteSelectorByAttribute.RouteAttributes   -> Seq(Seq.empty.asJava, "id", "geom", Seq("dtg", "geom").asJava).asJava
  ).asJava

  var h2Params: java.util.Map[String, _] = _

  var path: Path = _
  var ds: RoutedDataStoreView = _

  def comboParams(params: java.util.Map[String, _]*): java.util.Map[String, String] = {
    val configs = params.map(ConfigValueFactory.fromMap).asJava
    val config = ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(configs))
    Map(RoutedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())).asJava
  }

  step {
    path = Files.createTempDirectory(s"route-ds-test")

    h2Params = Map(
      "dbtype"                                 -> "h2",
      "database"                               -> path.toFile.getAbsolutePath,
      RouteSelectorByAttribute.RouteAttributes -> Seq("name", "age").asJava
    ).asJava

    val h2Ds = DataStoreFinder.getDataStore(h2Params)
    val accumuloDs = DataStoreFinder.getDataStore(accumuloParams)

    Seq(h2Ds, accumuloDs).foreach { ds =>
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
        features.iterator.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }

    foreach(Seq(h2Ds, accumuloDs)) { ds =>
      SelfClosingIterator(ds.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList must haveLength(10)
    }
    h2Ds.dispose()
    accumuloDs.dispose()

    ds = DataStoreFinder.getDataStore(comboParams(h2Params, accumuloParams)).asInstanceOf[RoutedDataStoreView]
    ds must not(beNull)
  }

  "MergedDataStoreView" should {
    "load multiple datastores" in {
      ds.getTypeNames mustEqual Array(sftName)
      ds.getNames.asScala mustEqual Seq(new NameImpl(sftName))

      val sft = ds.getSchema(sftName)

      sft.getAttributeCount mustEqual 4
      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("name", "age", "dtg", "geom")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[String], classOf[Integer], classOf[Date], classOf[Point])
    }

    "query multiple data stores" in {
      val results = SelfClosingIterator(ds.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList

      results must haveLength(10)
      foreach(results.sortBy(_.getAttribute(0).asInstanceOf[String]).zip(features)) { case (actual, expected) =>
        // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
        expected.getAttributes.asScala mustEqual actual.getAttributes.asScala
      }
    }

    "query multiple data stores with filters and transforms" in {
      val filters = Seq(
        "IN('3', '4', '5', '6')",
        "bbox(geom,44,52.5,46,56.5)",
        "bbox(geom,44,52,46,59) and dtg DURING 2018-01-01T00:02:30.000Z/2018-01-01T00:06:30.000Z",
        "name IN('name3', 'name4', 'name5', 'name6')"
      )
      val transforms = Seq(null, Array("geom"), Array("geom", "dtg"), Array("name"), Array("dtg", "geom", "age", "name"))

      foreach(filters) { filter =>
        val ecql = ECQL.toFilter(filter)
        foreach(transforms) { transform =>
          val query = new Query(sftName, ecql, transform)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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
    ds.dispose()
    PathUtils.deleteRecursively(path)
    allocator.close()
  }
}


