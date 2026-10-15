/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.feature.NameImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.gt.partition.postgis.TestWithSharedPostgisContainer
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.jts.geom.Point
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll

import java.nio.file.Files
import java.util.Date

class RoutedDataStoreViewTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  // note: shp seems to just use an incrementing number for the feature id,
  // and return `<typeName>.` when returning feature them
  // as such, we don't compare primary keys directly here

  private val sft =
    SimpleFeatureTypes.createType(getClass.getSimpleName, "*the_geom:Point:srid=4326,name:String:index=full,age:Int,dtg:Date")

  private val features = Seq.tabulate(9) { u =>
    val i = u + 1 // sync up with shp fid
    // shp seems to round dates to whole days??
    ScalaSimpleFeature.create(sft, s"$i", s"POINT (45 5$i)", s"name$i", 20 + i, s"2018-01-0${i}T00:00:00.000Z")
  }

  private val container = TestWithSharedPostgisContainer.container

  private lazy val postgisParams = Map(
    "dbtype" -> "postgis-partitioned",
    "host" -> container.getHost,
    "port" -> String.valueOf(container.getFirstMappedPort),
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> container.password,
    "Batch insert size" -> "10",
    RouteSelectorByAttribute.RouteAttributes -> java.util.List.of(java.util.List.of(), "id", "the_geom", java.util.List.of("dtg", "the_geom"))
  )

  private lazy val path = Files.createTempDirectory("route-ds-test")

  private lazy val shpParams = Map(
    "url" -> s"file://${path.toFile.getAbsolutePath}/${sft.getTypeName}.shp",
    RouteSelectorByAttribute.RouteAttributes -> java.util.List.of("name", "age")
  )

  private lazy val routedDs = DataStoreFinder.getDataStore(comboParams(shpParams, postgisParams)).asInstanceOf[RoutedDataStoreView]

  def comboParams(params: Map[String, _]*): java.util.Map[String, String] = {
    val configs = params.map(p => ConfigValueFactory.fromMap(p.asJava)).asJava
    val config = ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(configs))
    Map(RoutedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())).asJava
  }

  override def beforeAll(): Unit = {
    WithClose(DataStoreFinder.getDataStore(shpParams.asJava)) { shpDs =>
      WithClose(DataStoreFinder.getDataStore(postgisParams.asJava)) { postgisDs =>
        Seq(shpDs, postgisDs).foreach { ds =>
          ds.createSchema(sft)
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
        }
      }
    }

    routedDs must not(beNull)
  }

  override def afterAll(): Unit = {
    try {
      routedDs.dispose()
    } finally {
      PathUtils.deleteRecursively(path)
    }
  }

  "RoutedDataStoreView" should {
    "load multiple datastores" in {
      routedDs.getTypeNames mustEqual Array(sft.getTypeName)
      routedDs.getNames.asScala mustEqual Seq(new NameImpl(sft.getTypeName))

      val loadedSft = routedDs.getSchema(sft.getTypeName)

      loadedSft.getAttributeCount mustEqual 4
      loadedSft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("the_geom", "name", "age", "dtg")
      loadedSft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Point], classOf[String], classOf[Integer], classOf[Date])
    }

    "query multiple data stores" in {
      val results = CloseableIterator(routedDs.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList

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
          val query = new Query(sft.getTypeName, ecql, transform: _*)
          val results = CloseableIterator(routedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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
}


