/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.feature.NameImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.view.MergedDataStoreViewTest.TestConfigLoader
import org.locationtech.geomesa.process.analytic.DensityProcess
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.{BIN_ATTRIBUTE_INDEX, EncodedValues}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.MinMax
import org.locationtech.jts.geom.Point
import org.specs2.matcher.MatchResult
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.file.{Files, Path}
import java.util.Date

@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends TestWithFeatureType {

  import scala.collection.JavaConverters._

  // note: shp seems to just use `<typeName>.` plus an incrementing 1-based number for the feature id,
  // as such, we don't compare primary keys directly here

  sequential

  override val spec = "*the_geom:Point:srid=4326,name:String:index=full,age:Int,dtg:Date"

  lazy val features = Seq.tabulate(8) { u =>
    val i = u + 1 // sync up with shp fid
    // shp seems to round dates to whole days??
    ScalaSimpleFeature.create(sft, s"$i", s"POINT (45 5$i)", s"name$i", 20 + i, s"2018-01-0${i}T00:00:00.000Z")
  }

  val defaultFilter =
    ECQL.toFilter("bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-02T12:00:00.000Z/2018-01-06T12:00:00.000Z")

  val accumuloParams = dsParams.asJava

  var shpParams: java.util.Map[String, String] = _

  var path: Path = _
  var mergedDs: MergedDataStoreView = _

  def comboParams(params: java.util.Map[String, String]*): java.util.Map[String, String] = {
    val configs = params.map(ConfigValueFactory.fromMap).asJava
    val config = ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(configs))
    Map(MergedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())).asJava
  }

  step {
    path = Files.createTempDirectory(s"combo-ds-test")

    shpParams = Map(
      "url" -> s"file://${path.toFile.getAbsolutePath}/$sftName.shp"
    ).asJava

    val shpDs = DataStoreFinder.getDataStore(shpParams)
    val accumuloDs = DataStoreFinder.getDataStore(accumuloParams)

    val copied = features.iterator
    Seq(shpDs, accumuloDs).foreach { ds =>
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
        copied.take(4).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }

    SelfClosingIterator(shpDs.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList must haveLength(4)
    SelfClosingIterator(accumuloDs.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList must haveLength(4)

    shpDs.dispose()
    accumuloDs.dispose()

    mergedDs = DataStoreFinder.getDataStore(comboParams(shpParams, accumuloParams)).asInstanceOf[MergedDataStoreView]
    mergedDs must not(beNull)
  }

  "MergedDataStoreView" should {
    "respect max features" in {
      val query = new Query(sftName)
      query.setMaxFeatures(1)
      query.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)
      mergedDs.getFeatureSource(sft.getTypeName).getCount(query) mustEqual 1
      SelfClosingIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList must haveLength(1)
    }

    "load multiple datastores" in {
      mergedDs.getTypeNames mustEqual Array(sftName)
      mergedDs.getNames.asScala mustEqual Seq(new NameImpl(sftName))

      val sft = mergedDs.getSchema(sftName)

      sft.getAttributeCount mustEqual 4
      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("the_geom", "name", "age", "dtg")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Point], classOf[String], classOf[Integer], classOf[Date])
    }

    "load via SPI config" in {
      val shpConfig = ConfigValueFactory.fromMap(shpParams)
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
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(shpConfig, accumuloConfig).asJava))
      testParams(ConfigFactory.empty())

      MergedDataStoreViewTest.loadConfig =
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(shpConfig).asJava))
      testParams(ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(accumuloConfig).asJava)))

      MergedDataStoreViewTest.loadConfig =
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(accumuloConfig).asJava))
      testParams(ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(shpConfig).asJava)))
    }

    "handle unquoted store keys" in {
      def unquotedConfig(params: Map[String, String]): Config = {
        params.foldLeft(ConfigFactory.empty()) { case (conf, (k, v)) =>
          conf.withValue(k, ConfigValueFactory.fromAnyRef(v))
        }
      }
      val shpConfig = unquotedConfig(shpParams.asScala.toMap).root()
      val accumuloConfig = unquotedConfig(accumuloParams.asScala.toMap).root()
      val config =
        ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(accumuloConfig, shpConfig).asJava))

      val params = Map(
        MergedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())
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

    "query multiple data stores" in {
      val results = SelfClosingIterator(mergedDs.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)).toList

      results must haveLength(features.length)
      foreach(results.sortBy(_.getAttribute("name").asInstanceOf[String]).zip(features)) { case (actual, expected) =>
        // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
        expected.getAttributes.asScala mustEqual actual.getAttributes.asScala
      }
    }

    "query multiple data stores with filters and transforms" in {
      val filters = Seq(
        s"IN('$sftName.3', '$sftName.4', '5', '6')",
        "bbox(the_geom,44,52.5,46,56.5)",
        "bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-02T12:00:00.000Z/2018-01-06T12:00:00.000Z",
        "name IN('name3', 'name4', 'name5', 'name6') and bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-01T12:00:00.000Z/2018-01-07T12:00:00.000Z"
      )
      val transforms = Seq(null, Array("the_geom"), Array("the_geom", "dtg"), Array("name"), Array("dtg", "the_geom", "age", "name"))

      foreach(filters) { filter =>
        val ecql = ECQL.toFilter(filter)
        foreach(transforms) { transform =>
          val query = new Query(sftName, ecql, transform: _*)
          val results = SelfClosingIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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
        s"IN('$sftName.3', '$sftName.4', '5', '6')",
        "bbox(the_geom,44,52.5,46,56.5)",
        "bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-02T12:00:00.000Z/2018-01-06T12:00:00.000Z",
        "name IN('name3', 'name4', 'name5', 'name6') and bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-01T12:00:00.000Z/2018-01-07T12:00:00.000Z"
      )

      // these filters exclude the '5' feature
      val shpFilteredParams = new java.util.HashMap[String, String](shpParams)
      shpFilteredParams.put(MergedDataStoreViewFactory.StoreFilterParam.key, "dtg < '2018-01-06T00:00:00.000Z'")
      val accumuloFilteredParams = new java.util.HashMap[String, String](accumuloParams)
      accumuloFilteredParams.put(MergedDataStoreViewFactory.StoreFilterParam.key, "dtg >= '2018-01-06T00:00:00.000Z'")

      val ds = DataStoreFinder.getDataStore(comboParams(shpFilteredParams, accumuloFilteredParams))
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
      foreach(Seq[Array[String]](null, Array("the_geom", "dtg"))) { transform =>
        val query = new Query(sftName, Filter.INCLUDE, transform: _*)
        query.setSortBy(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.DESCENDING))
        val results = SelfClosingIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

        results must haveLength(features.length)
        // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
        foreach(results.map(_.getAttribute("dtg")).zip(features.reverse.map(_.getAttribute("dtg")))) {
          case (actual, expected) => expected mustEqual actual
        }
      }
    }

    "query multiple data stores and return bins" in {
      val query = new Query(sftName, defaultFilter, "name", "dtg", "the_geom")
      query.getHints.put(QueryHints.BIN_TRACK, "name")

      val bytes = SelfClosingIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).map { f =>
        val array = f.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]]
        val copy = Array.ofDim[Byte](array.length)
        System.arraycopy(array, 0, copy, 0, array.length)
        copy
      }

      val expected = features.slice(2, 6).map { f =>
        val pt = f.getDefaultGeometry.asInstanceOf[Point]
        val time = f.getAttribute("dtg").asInstanceOf[Date].getTime
        EncodedValues(f.getAttribute("name").hashCode, pt.getY.toFloat, pt.getX.toFloat, time, -1)
      }

      val results = bytes.flatMap(_.grouped(16).map(BinaryOutputEncoder.decode)).toList
      results.sortBy(_.dtg) mustEqual expected
    }

    "query multiple data stores and return arrow" in {
      val query = new Query(sftName, defaultFilter, "name", "dtg", "the_geom")
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
      val results = SelfClosingIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        val expected = features.slice(2, 6)
        reader.dictionaries.keySet mustEqual Set("name")
        reader.dictionaries.apply("name").iterator.toSeq must containAllOf(expected.map(_.getAttribute("name")))
        val results = SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toList
        results must haveLength(4)
        foreach(results.zip(expected)) { case (actual, e) =>
          actual.getAttributeCount mustEqual 3
          foreach(Seq("name", "dtg", "the_geom")) { attribute =>
            actual.getAttribute(attribute) mustEqual e.getAttribute(attribute)
          }
        }
      }
    }

    "query multiple data stores for stats" in {
      foreach(Seq[Array[String]](null, Array("dtg"))) { transform =>
        val query = new Query(sftName, defaultFilter, transform: _*)
        query.getHints.put(QueryHints.STATS_STRING, "MinMax(dtg)")
        query.getHints.put(QueryHints.ENCODE_STATS, true)

        val results = SelfClosingIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
        results must haveLength(1)

        val stat = StatsScan.decodeStat(sft)(results.head.getAttribute(0).asInstanceOf[String])
        stat must beAnInstanceOf[MinMax[Date]]
        stat.asInstanceOf[MinMax[Date]].min mustEqual features(2).getAttribute("dtg")
        stat.asInstanceOf[MinMax[Date]].max mustEqual features(5).getAttribute("dtg")
      }
    }

    "query multiple data stores for heatmaps" in {
      val envelope = new ReferencedEnvelope(44, 46, 52, 59, CRS_EPSG_4326)
      val width = 480
      val height = 360

      val query = new Query(sftName, defaultFilter, "the_geom")
      query.getHints.put(QueryHints.DENSITY_BBOX, envelope)
      query.getHints.put(QueryHints.DENSITY_WIDTH, width)
      query.getHints.put(QueryHints.DENSITY_HEIGHT, height)

      val decode = DensityScan.decodeResult(envelope, width, height)

      val results = SelfClosingIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT))

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
        null,
        envelope,
        width,
        height,
        new Query(sftName, defaultFilter),
        null
      )

      val coverage = process.execute(
        mergedDs.getFeatureSource(sftName).getFeatures(query),
        radiusPixels,
        null,
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
    mergedDs.dispose()
    PathUtils.deleteRecursively(path)
  }
}

object MergedDataStoreViewTest {

  private var loadConfig: Config = _

  class TestConfigLoader extends MergedViewConfigLoader {
    override def load(): Config = loadConfig
  }
}
