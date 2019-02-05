/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.file.{Files, Path}
import java.util.Date

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.feature.NameImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.view.MergedDataStoreViewTest.TestConfigLoader
import org.locationtech.geomesa.process.analytic.DensityProcess
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.{BIN_ATTRIBUTE_INDEX, EncodedValues}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.MinMax
import org.locationtech.jts.geom.Point
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortOrder
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends Specification {

  import scala.collection.JavaConverters._

  // note: h2 seems to require ints as the primary key, and then prepends `<typeName>.` when returning them
  // as such, we don't compare primary keys directly here
  // there may be a way to override this behavior but I haven't found it...

  sequential // note: shouldn't need to be sequential, but h2 doesn't do well with concurrent requests

  // we use class name to prevent spillage between unit tests in the mock connector
  val sftName: String = getClass.getSimpleName
  val spec = "name:String:index=full,age:Int,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType(sftName, spec)

  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", 20 + i, s"2018-01-01T00:0$i:00.000Z", s"POINT (45 5$i)")
  }

  val defaultFilter = ECQL.toFilter("bbox(geom,44,52,46,59) and dtg DURING 2018-01-01T00:02:30.000Z/2018-01-01T00:06:30.000Z")

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val accumuloParams = Map(
    AccumuloDataStoreParams.InstanceIdParam.key -> "mycloud",
    AccumuloDataStoreParams.ZookeepersParam.key -> "myzoo",
    AccumuloDataStoreParams.UserParam.key       -> "user",
    AccumuloDataStoreParams.PasswordParam.key   -> "password",
    AccumuloDataStoreParams.CatalogParam.key    -> sftName,
    AccumuloDataStoreParams.MockParam.key       -> "true"
  ).asJava

  var h2Params: java.util.Map[String, String] = _

  var path: Path = _
  var ds: MergedDataStoreView = _

  def comboParams(params: java.util.Map[String, String]*): java.util.Map[String, String] = {
    val configs = params.map(ConfigValueFactory.fromMap).asJava
    val config = ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(configs))
    Map(MergedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())).asJava
  }

  step {
    path = Files.createTempDirectory(s"combo-ds-test")

    h2Params = Map(
      "dbtype"   -> "h2",
      "database" -> path.toFile.getAbsolutePath
    ).asJava

    val h2Ds = DataStoreFinder.getDataStore(h2Params)
    val accumuloDs = DataStoreFinder.getDataStore(accumuloParams)

    val copied = features.iterator
    Seq(h2Ds, accumuloDs).foreach { ds =>
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
        copied.take(5).foreach { copy =>
          FeatureUtils.copyToWriter(writer, copy, useProvidedFid = true)
          writer.write()
        }
      }
    }

    foreach(Seq(h2Ds, accumuloDs)) { ds =>
      SelfClosingIterator(ds.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList must haveLength(5)
    }
    h2Ds.dispose()
    accumuloDs.dispose()

    ds = DataStoreFinder.getDataStore(comboParams(h2Params, accumuloParams)).asInstanceOf[MergedDataStoreView]
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

    "load via SPI config" in {
      val h2Config = ConfigValueFactory.fromMap(h2Params)
      val accumuloConfig = ConfigValueFactory.fromMap(accumuloParams)

      def testParams(config: Config): MatchResult[Any] = {
        val params = Map(
          MergedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise()),
          MergedDataStoreViewFactory.ConfigLoaderParam.get.key -> classOf[TestConfigLoader].getName
        )
        val ds = DataStoreFinder.getDataStore(params.asJava)
        ds must not(beNull)
        try {
          ds must beAnInstanceOf[MergedDataStoreView]
          ds.asInstanceOf[MergedDataStoreView].stores must haveLength(2)
        } finally {
          ds.dispose()
        }
      }

      MergedDataStoreViewTest.loadConfig =
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(h2Config, accumuloConfig).asJava))
      testParams(ConfigFactory.empty())

      MergedDataStoreViewTest.loadConfig =
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(h2Config).asJava))
      testParams(ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(accumuloConfig).asJava)))

      MergedDataStoreViewTest.loadConfig =
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(accumuloConfig).asJava))
      testParams(ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(h2Config).asJava)))
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
        "name IN('name3', 'name4', 'name5', 'name6') and bbox(geom,44,52,46,59) and dtg DURING 2018-01-01T00:01:30.000Z/2018-01-01T00:07:30.000Z"
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

    "apply filters to each data store impl" in {
      val filters = Seq(
        "IN('3', '4', '5', '6')",
        "bbox(geom,44,52.5,46,56.5)",
        "bbox(geom,44,52,46,59) and dtg DURING 2018-01-01T00:02:30.000Z/2018-01-01T00:06:30.000Z",
        "name IN('name3', 'name4', 'name5', 'name6') and bbox(geom,44,52,46,59) and dtg DURING 2018-01-01T00:01:30.000Z/2018-01-01T00:07:30.000Z"
      )

      // these filters exclude the '5' feature
      val h2FilteredParams = new java.util.HashMap[String, String](h2Params)
      h2FilteredParams.put(MergedDataStoreViewFactory.StoreFilterParam.key, "dtg < '2018-01-01T00:06:00.000Z'")
      val accumuloFilteredParams = new java.util.HashMap[String, String](accumuloParams)
      accumuloFilteredParams.put(MergedDataStoreViewFactory.StoreFilterParam.key, "dtg >= '2018-01-01T00:06:00.000Z'")

      val ds = DataStoreFinder.getDataStore(comboParams(h2FilteredParams, accumuloFilteredParams))
      try {
        foreach(filters) { filter =>
          val ecql = ECQL.toFilter(filter)
          val query = new Query(sftName, ecql)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          results must haveLength(3)
          forall(results) { feature =>
            // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
            features.find(f => feature.getID.contains(f.getID)).get.getAttributes mustEqual feature.getAttributes
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "query multiple data stores with sorting" in {
      foreach(Seq[Array[String]](null, Array("geom", "dtg"))) { transform =>
        val query = new Query(sftName, Filter.INCLUDE, transform)
        query.setSortBy(Array(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.DESCENDING)))
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

        results must haveLength(10)
        // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
        foreach(results.map(_.getAttribute("dtg")).zip(features.reverse.map(_.getAttribute("dtg")))) {
          case (actual, expected) => expected mustEqual actual
        }
      }
    }

    "query multiple data stores and return bins" in {
      val query = new Query(sftName, defaultFilter, Array("name", "dtg", "geom"))
      query.getHints.put(QueryHints.BIN_TRACK, "name")

      val bytes = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).map { f =>
        val array = f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]
        val copy = Array.ofDim[Byte](array.length)
        System.arraycopy(array, 0, copy, 0, array.length)
        copy
      }

      val expected = features.slice(3, 7).map { f =>
        val pt = f.getDefaultGeometry.asInstanceOf[Point]
        val time = f.getAttribute("dtg").asInstanceOf[Date].getTime
        EncodedValues(f.getAttribute("name").hashCode, pt.getY.toFloat, pt.getX.toFloat, time, -1)
      }

      val results = bytes.flatMap(_.grouped(16).map(BinaryOutputEncoder.decode)).toList
      results.sortBy(_.dtg) mustEqual expected
    }

    "query multiple data stores and return arrow" in {
      val query = new Query(sftName, defaultFilter, Array("name", "dtg", "geom"))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        val expected = features.slice(3, 7)
        reader.dictionaries.keySet mustEqual Set("name")
        reader.dictionaries.apply("name").iterator.toSeq must containAllOf(expected.map(_.getAttribute("name")))
        val results = SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toList
        results must haveLength(4)
        foreach(results.zip(expected)) { case (actual, e) =>
          actual.getAttributeCount mustEqual 3
          foreach(Seq("name", "dtg", "geom")) { attribute =>
            actual.getAttribute(attribute) mustEqual e.getAttribute(attribute)
          }
        }
      }
    }

    "query multiple data stores for stats" in {
      foreach(Seq[Array[String]](null, Array("dtg"))) { transform =>
        val query = new Query(sftName, defaultFilter, transform)
        query.getHints.put(QueryHints.STATS_STRING, "MinMax(dtg)")
        query.getHints.put(QueryHints.ENCODE_STATS, true)

        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
        results must haveLength(1)

        val stat = StatsScan.decodeStat(sft)(results.head.getAttribute(0).asInstanceOf[String])
        stat must beAnInstanceOf[MinMax[Date]]
        stat.asInstanceOf[MinMax[Date]].min mustEqual features(3).getAttribute("dtg")
        stat.asInstanceOf[MinMax[Date]].max mustEqual features(6).getAttribute("dtg")
      }
    }

    "query multiple data stores for heatmaps" in {
      val envelope = new ReferencedEnvelope(44, 46, 52, 59, CRS_EPSG_4326)
      val width = 480
      val height = 360

      val query = new Query(sftName, defaultFilter, Array("geom"))
      query.getHints.put(QueryHints.DENSITY_BBOX, envelope)
      query.getHints.put(QueryHints.DENSITY_WIDTH, width)
      query.getHints.put(QueryHints.DENSITY_HEIGHT, height)

      val decode = DensityScan.decodeResult(envelope, width, height)

      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))

      def round(f: Double): Double = math.round(f * 10) / 10d

      val counts = results.flatMap(decode.apply).foldLeft(Map.empty[(Double, Double), Double]) {
        case (map, (x, y, weight)) => map + ((round(x), round(y)) -> weight)
      }

      val expected = Seq((45.0, 53.0) -> 1.0, (45.0, 54.0) -> 1.0, (45.0, 55.0) -> 1.0, (45.0, 56.0) -> 1.0)

      counts must haveSize(4)
      counts must containTheSameElementsAs(expected)
    }

    "query multiple data stores through density process" in {
      val process = new DensityProcess()
      val radiusPixels = 10
      val envelope = new ReferencedEnvelope(44, 46, 52, 59, CRS_EPSG_4326)
      val width = 480
      val height = 360

      val query = process.invertQuery(
        radiusPixels,
        null,
        envelope,
        width,
        height,
        new Query(sftName, defaultFilter),
        null
      )

      val coverage = process.execute(
        ds.getFeatureSource(sftName).getFeatures(query),
        radiusPixels,
        null,
        envelope,
        width,
        height,
        null
      )

      // TODO way to test coverage?
      coverage must not(beNull)

      coverage.dispose(false)
      ok
    }
  }

  step {
    ds.dispose()
    PathUtils.deleteRecursively(path)
    allocator.close()
  }
}

object MergedDataStoreViewTest {

  private var loadConfig: Config = _

  class TestConfigLoader extends MergedViewConfigLoader {
    override def load(): Config = loadConfig
  }
}
