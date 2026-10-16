/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.feature.NameImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.gt.partition.postgis.{PostgisContainer, TestWithSharedPostgisContainer}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.stats.impl.MinMax
import org.locationtech.geomesa.index.view.MergedDataStoreViewTest.TestConfigLoader
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.{BIN_ATTRIBUTE_INDEX, EncodedValues}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.jts.geom.Point
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.util.Date

class MergedDataStoreViewTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  // note: shp seems to just use `<typeName>.` plus an incrementing 1-based number for the feature id,
  // as such, we don't compare primary keys directly here

  private val sft =
    SimpleFeatureTypes.createType(getClass.getSimpleName, "*the_geom:Point:srid=4326,name:String:index=full,age:Int,dtg:Date")

  private val features = Seq.tabulate(8) { u =>
    val i = u + 1 // sync up with shp fid
    // shp seems to round dates to whole days??
    ScalaSimpleFeature.create(sft, s"$i", s"POINT (45 5$i)", s"name$i", 20 + i, s"2018-01-0${i}T00:00:00.000Z")
  }

  private val defaultFilter =
    ECQL.toFilter("bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-02T12:00:00.000Z/2018-01-06T12:00:00.000Z")

  private val container = TestWithSharedPostgisContainer.container

  private lazy val postgisParams = Map(
    "dbtype" -> "postgis-partitioned",
    "host" -> container.getHost,
    "port" -> String.valueOf(container.getFirstMappedPort),
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> container.password,
    "Batch insert size" -> "10",
  )

  private lazy val path = Files.createTempDirectory("combo-ds-test")

  private lazy val shpParams = Map(
    "url" -> s"file://${path.toFile.getAbsolutePath}/${sft.getTypeName}.shp"
  )

  private lazy val mergedDs = DataStoreFinder.getDataStore(comboParams(shpParams, postgisParams)).asInstanceOf[MergedDataStoreView]

  override def beforeAll(): Unit = {
    WithClose(DataStoreFinder.getDataStore(shpParams.asJava)) { shpDs =>
      WithClose(DataStoreFinder.getDataStore(postgisParams.asJava)) { postgisDs =>
        val copied = features.iterator
        Seq(shpDs, postgisDs).foreach { ds =>
          ds.createSchema(sft)
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            copied.take(4).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
        }
      }
    }

    mergedDs must not(beNull)
  }

  override def afterAll(): Unit = {
    try {
      // clean up our shared container
      WithClose(DataStoreFinder.getDataStore(postgisParams.asJava))(_.removeSchema(sft.getTypeName))
      mergedDs.dispose()
    } finally {
      PathUtils.deleteRecursively(path)
    }
  }

  def comboParams(params: Map[String, String]*): java.util.Map[String, String] = {
    val configs = params.map(p => ConfigValueFactory.fromMap(p.asJava)).asJava
    val config = ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(configs))
    Map(MergedDataStoreViewFactory.ConfigParam.key -> config.root.render(ConfigRenderOptions.concise())).asJava
  }

  "MergedDataStoreView" should {
    "respect max features" in {
      val query = new Query(sft.getTypeName)
      query.setMaxFeatures(1)
      query.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)
      mergedDs.getFeatureSource(sft.getTypeName).getCount(query) mustEqual 1
      CloseableIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList must haveLength(1)
    }

    "load multiple datastores" in {
      mergedDs.getTypeNames mustEqual Array(sft.getTypeName)
      mergedDs.getNames.asScala mustEqual Seq(new NameImpl(sft.getTypeName))

      val loadedSft = mergedDs.getSchema(sft.getTypeName)

      loadedSft.getAttributeCount mustEqual 4
      loadedSft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("the_geom", "name", "age", "dtg")
      loadedSft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Point], classOf[String], classOf[Integer], classOf[Date])
    }

    "load via SPI config" in {
      val shpConfig = ConfigValueFactory.fromMap(shpParams.asJava)
      val postgisConfig = ConfigValueFactory.fromMap(postgisParams.asJava)

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
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(shpConfig, postgisConfig).asJava))
      testParams(ConfigFactory.empty())

      MergedDataStoreViewTest.loadConfig =
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(shpConfig).asJava))
      testParams(ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(postgisConfig).asJava)))

      MergedDataStoreViewTest.loadConfig =
          ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(postgisConfig).asJava))
      testParams(ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(shpConfig).asJava)))
    }

    "handle unquoted store keys" in {
      def unquotedConfig(params: Map[String, String]): Config = {
        params.foldLeft(ConfigFactory.empty()) { case (conf, (k, v)) =>
          conf.withValue(k, ConfigValueFactory.fromAnyRef(v))
        }
      }
      val shpConfig = unquotedConfig(shpParams).root()
      val postgisConfig = unquotedConfig(postgisParams).root()
      val config =
        ConfigFactory.empty().withValue("stores", ConfigValueFactory.fromIterable(Seq(postgisConfig, shpConfig).asJava))

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
      val results = CloseableIterator(mergedDs.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList

      results must haveLength(features.length)
      foreach(results.sortBy(_.getAttribute("name").asInstanceOf[String]).zip(features)) { case (actual, expected) =>
        // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
        expected.getAttributes.asScala mustEqual actual.getAttributes.asScala
      }
    }

    "query multiple data stores with filters and transforms" in {
      val filters = Seq(
        s"IN('${sft.getTypeName}.3', '${sft.getTypeName}.4', '5', '6')",
        "bbox(the_geom,44,52.5,46,56.5)",
        "bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-02T12:00:00.000Z/2018-01-06T12:00:00.000Z",
        "name IN('name3', 'name4', 'name5', 'name6') and bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-01T12:00:00.000Z/2018-01-07T12:00:00.000Z"
      )
      val transforms = Seq(null, Array("the_geom"), Array("the_geom", "dtg"), Array("name"), Array("dtg", "the_geom", "age", "name"))

      foreach(filters) { filter =>
        val ecql = ECQL.toFilter(filter)
        foreach(transforms) { transform =>
          val query = new Query(sft.getTypeName, ecql, transform: _*)
          val results = CloseableIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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
        s"IN('${sft.getTypeName}.3', '${sft.getTypeName}.4', '5', '6')",
        "bbox(the_geom,44,52.5,46,56.5)",
        "bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-02T12:00:00.000Z/2018-01-06T12:00:00.000Z",
        "name IN('name3', 'name4', 'name5', 'name6') and bbox(the_geom,44,52,46,59) and dtg DURING 2018-01-01T12:00:00.000Z/2018-01-07T12:00:00.000Z"
      )

      val keys = Seq(
        MergedDataStoreViewFactory.StoreFilterParam.key,
        s"${MergedDataStoreViewFactory.StoreFilterParam.key}.${sft.getTypeName}"
      )
      foreach(keys) { key =>
        // these filters exclude the '5' feature
        val shpFilteredParams = shpParams ++ Map(key -> "dtg < '2018-01-06T00:00:00.000Z'")
        val postgisFilteredParams = postgisParams ++ Map(key -> "dtg >= '2018-01-06T00:00:00.000Z'")
        val ds = DataStoreFinder.getDataStore(comboParams(shpFilteredParams, postgisFilteredParams))
        try {
          foreach(filters) { filter =>
            val ecql = ECQL.toFilter(filter)
            val query = new Query(sft.getTypeName, ecql)
            val results = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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
    }

    "query multiple data stores with sorting" in {
      foreach(Seq[Array[String]](null, Array("the_geom", "dtg"))) { transform =>
        val query = new Query(sft.getTypeName, Filter.INCLUDE, transform: _*)
        query.setSortBy(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.DESCENDING))
        val results = CloseableIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

        results must haveLength(features.length)
        // note: have to compare backwards as java.sql.Timestamp.equals(java.util.Date) always returns false
        foreach(results.map(_.getAttribute("dtg")).zip(features.reverse.map(_.getAttribute("dtg")))) {
          case (actual, expected) => expected mustEqual actual
        }
      }
    }

    "query multiple data stores and return bins" in {
      val query = new Query(sft.getTypeName, defaultFilter, "name", "dtg", "the_geom")
      query.getHints.put(QueryHints.BIN_TRACK, "name")

      val bytes = CloseableIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).map { f =>
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
      val query = new Query(sft.getTypeName, defaultFilter, "name", "dtg", "the_geom")
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
      val out = new ByteArrayOutputStream
      CloseableIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT))
        .foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      WithClose(SimpleFeatureArrowFileReader.streaming(out.toByteArray)) { reader =>
        val expected = features.slice(2, 6)
        reader.dictionaries.keySet mustEqual Set("name")
        reader.dictionaries.apply("name").iterator.toSeq must containAllOf(expected.map(_.getAttribute("name")))
        val results = CloseableIterator(reader.features()).map(ScalaSimpleFeature.copy).toList
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
        val query = new Query(sft.getTypeName, defaultFilter, transform: _*)
        query.getHints.put(QueryHints.STATS_STRING, "MinMax(dtg)")
        query.getHints.put(QueryHints.ENCODE_STATS, true)

        val results = CloseableIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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

      val query = new Query(sft.getTypeName, defaultFilter, "the_geom")
      query.getHints.put(QueryHints.DENSITY_BBOX, envelope)
      query.getHints.put(QueryHints.DENSITY_WIDTH, width)
      query.getHints.put(QueryHints.DENSITY_HEIGHT, height)

      val decode = DensityScan.decodeResult(envelope, width, height)

      val results = CloseableIterator(mergedDs.getFeatureReader(query, Transaction.AUTO_COMMIT))

      def round(f: Double): Double = math.round(f * 10) / 10d

      val counts = results.flatMap(decode.apply).toList.foldLeft(Map.empty[(Double, Double), Double]) {
        case (map, (x, y, weight)) => map + ((round(x), round(y)) -> weight)
      }

      val expected = Seq((45.0, 53.0) -> 1.0, (45.0, 54.0) -> 1.0, (45.0, 55.0) -> 1.0, (45.0, 56.0) -> 1.0)

      counts must haveSize(4)
      counts must containTheSameElementsAs(expected)
    }

    "query multiple data stores through density process" in {
      // code copied from DensityProcess.invertQuery, but we don't want to pull in that dependency here
      val pixels = 10
      // buffered output
      val outputWidth = 480 + 2 * pixels
      val outputHeight = 360 + 2 * pixels

      // slightly buffer the envelope so it's covered with our default filter
      val envelope = new ReferencedEnvelope(43.9, 46.1, 51.9, 59.1, CRS_EPSG_4326)

      val query = new Query(sft.getTypeName, defaultFilter)
      query.getHints.put(QueryHints.DENSITY_BBOX, envelope)
      query.getHints.put(QueryHints.DENSITY_WIDTH, outputWidth)
      query.getHints.put(QueryHints.DENSITY_HEIGHT, outputHeight)

      // we run the decode logic from density process as we don't have coverage/jai on the classpath in order to run the whole thing
      val points = scala.collection.mutable.Map.empty[(Double, Double), Double]
      val decode = DensityScan.decodeResult(envelope, outputWidth, outputHeight)
      WithClose(mergedDs.getFeatureSource(sft.getTypeName).getFeatures(query).features()) { features =>
        while (features.hasNext) {
          val pts = decode(features.next())
          while (pts.hasNext) {
            val (x, y, weight) = pts.next()
            points.put((x, y), points.getOrElse((x, y), 0d) + weight)
          }
        }
      }
      // TODO way to test coverage?
      points must not(beEmpty)
    }
  }
}

object MergedDataStoreViewTest {

  private var loadConfig: Config = _

  class TestConfigLoader extends MergedViewConfigLoader {
    override def load(): Config = loadConfig
  }
}
