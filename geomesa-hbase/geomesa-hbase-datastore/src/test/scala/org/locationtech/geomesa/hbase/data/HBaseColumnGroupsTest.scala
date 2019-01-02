/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Date

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Envelope, Point}
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, HBaseCatalogParam}
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.CoprocessorPlan
import org.locationtech.geomesa.index.conf.QueryHints.{BIN_BATCH_SIZE, BIN_TRACK}
import org.locationtech.geomesa.index.conf.{ColumnGroups, QueryHints}
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.{QueryPlanner, Transforms}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodedValues
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.{MinMax, Stat}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

class HBaseColumnGroupsTest extends HBaseTest with LazyLogging  {

  import scala.collection.JavaConverters._

  // note: using Seq.foreach, ok instead of foreach(Seq) shaves several seconds off the time to run this test

  var ds: HBaseDataStore = _

  val spec: String = "name:String:index=true:column-groups=B,age:Int:index=true:column-groups=B," +
      "height:Double,track:String:column-groups=A,dtg:Date:column-groups=A,*geom:Point:srid=4326:column-groups='A,B'"

  val sft = SimpleFeatureTypes.createType(getClass.getSimpleName, spec)

  val features = IndexedSeq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", i * 10, 60 + i, s"track-${i % 3}",
      s"2018-01-01T0$i:00:01.000Z", s"POINT (45.$i 55)")
  }

  val transformCache = Caffeine.newBuilder().build(
    new CacheLoader[Array[String], (SimpleFeatureType, Seq[Expression])]() {
      override def load(transform: Array[String]): (SimpleFeatureType, Seq[Expression]) = {
        val (tdefs, tsft) = QueryPlanner.buildTransformSFT(sft, transform)
        (tsft, Transforms.definitions(tdefs).map(_.expression))
      }
    }
  )

  implicit class RichQuery(query: Query) {
    def toList: List[SimpleFeature] =
      SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
  }

  implicit class RichResult(expected: Seq[Int]) {
    def toFeatures(transform: Array[String]): Seq[SimpleFeature] = {
      if (transform == null) { expected.map(features.apply) } else {
        expected.map(features.apply).toList.map { f =>
          val (tsft, expressions) = transformCache.get(transform)
          ScalaSimpleFeature.create(tsft, f.getID, expressions.map(_.evaluate(f)): _*)
        }
      }
    }
  }

  private def toFilter(f: (String, Seq[Int])): (Filter, Seq[Int]) = f.copy(_1 = ECQL.toFilter(f._1))

  // filters that can be satisfied by the various column groups (assumes strict bbox)

  val filtersA = Seq(
    ("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z and bbox(geom,45.4,54.9,45.8,55.1)", 4 to 8),
    ("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z", 0 to 8)
  ).map(toFilter)

  val filtersB = Seq(
    ("age >= 50 AND name IN ('name1', 'name2')", Seq.empty)
  ).map(toFilter)

  val filtersDefault = Seq(
    ("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z and bbox(geom,45.4,54.9,45.8,55.1) and height < 67", 4 to 6),
    ("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z and height < 67", 0 to 6),
    ("bbox(geom,45.4,54.9,45.8,55.1) and height < 67", 4 to 6),
    ("age >= 50 and height < 67", 5 to 6),
    ("bbox(geom,45.4,54.9,45.8,55.1) AND name IN ('name5', 'name6') and height < 67", 5 to 6),
    ("bbox(geom,45.4,54.9,45.8,55.1) AND age >= 50 and height < 67", 5 to 6),
    ("age >= 50 AND name IN ('name1', 'name2') and height < 67", Seq.empty)
  ).map(toFilter)

  // transforms that can be satisfied by the various column groups

  val transformsA = Seq(Array("dtg", "geom"), Array("dtg"), Array("derived=buffer(geom, 0.1)", "dtg"))

  val transformsB = Seq(Array("name", "age", "geom"), Array("age", "geom"), Array("name", "geom"),
    Array("name", "age"), Array("derived=strConcat('foo-', name)", "geom"))

  val transformsAB = Seq(Array("geom"))

  val transformsDefault = Seq(null: Array[String], Array("height", "dtg", "geom"), Array("height", "geom"),
    Array("age", "height", "geom"), Array("name", "height", "geom"), Array("name", "height", "age"),
    Array("derived=strConcat('foo-', height)", "dtg"))

  step {
    logger.info("Starting HBase column groups test")
    val params = Map(ConnectionParam.getName -> connection, HBaseCatalogParam.getName -> catalogTableName)
    ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
    ds.createSchema(sft)
    val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
    features.foreach { f =>
      FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
      writer.write()
    }
    writer.close()
  }

  "HBaseDataStore column groups" should {
    "create column groups" in {
      val tables = ds.getAllIndexTableNames(sft.getTypeName)
      tables must not(beEmpty)
      foreach(tables) { table =>
        val fams = ds.connection.getTable(TableName.valueOf(table)).getTableDescriptor.getColumnFamilies
        fams.toSeq.map(_.getNameAsString) must containAllOf(Seq("A", "B"))
      }
    }
    "use minimal column groups required by the filter and transform, group a" in {
      filtersA.foreach { case (filter, expected) =>
        (transformsA ++ transformsAB).foreach { transform =>
          val query = new Query(sft.getTypeName, filter, transform)
          query.getHints.put(QueryHints.LOOSE_BBOX, false)
          foreach(ds.getQueryPlan(query).flatMap(_.scans))(_.getFamilies.map(Bytes.toString) mustEqual Array("A"))
          query.toList mustEqual expected.toFeatures(transform)
        }
        (transformsB ++ transformsDefault).foreach { transform =>
          val query = new Query(sft.getTypeName, filter, transform)
          query.getHints.put(QueryHints.LOOSE_BBOX, false)
          foreach(ds.getQueryPlan(query).flatMap(_.scans))(_.getFamilies mustEqual Array(ColumnGroups.Default))
          query.toList mustEqual expected.toFeatures(transform)
        }
      }
      ok
    }
    "use minimal column groups required by the filter and transform, group b" in {
      filtersB.foreach { case (filter, expected) =>
        (transformsB ++ transformsAB).foreach { transform =>
          val query = new Query(sft.getTypeName, filter, transform)
          query.getHints.put(QueryHints.LOOSE_BBOX, false)
          foreach(ds.getQueryPlan(query).flatMap(_.scans))(_.getFamilies.map(Bytes.toString) mustEqual Array("B"))
//          foreach(ds.getQueryPlan(query).flatMap(_.ranges)) { r =>
//              if (r.getFamilies.map(Bytes.toString).contains("A")) {
//                println(ECQL.toCQL(filter) + " " + transform.mkString(","))
//              }
//            r.getFamilies.map(Bytes.toString) mustEqual Array("B")
//          }
          query.toList mustEqual expected.toFeatures(transform)
        }
        (transformsA ++ transformsDefault).foreach { transform =>
          val query = new Query(sft.getTypeName, filter, transform)
          query.getHints.put(QueryHints.LOOSE_BBOX, false)
          foreach(ds.getQueryPlan(query).flatMap(_.scans))(_.getFamilies mustEqual Array(ColumnGroups.Default))
          query.toList mustEqual expected.toFeatures(transform)
        }
      }
      ok
    }
    "use minimal column groups required by the filter and transform, default group" in {
      filtersDefault.foreach { case (filter, expected) =>
        (transformsA ++ transformsB ++ transformsAB ++ transformsDefault).foreach { transform =>
          val query = new Query(sft.getTypeName, filter, transform)
          query.getHints.put(QueryHints.LOOSE_BBOX, false)
          foreach(ds.getQueryPlan(query).flatMap(_.scans))(_.getFamilies mustEqual Array(ColumnGroups.Default))
          query.toList mustEqual expected.toFeatures(transform)
        }
      }
      ok
    }
    "work with arrow queries" in {
      val filter = ECQL.toFilter("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z " +
          "and bbox(geom,45.4,54.9,45.8,55.1)")

      val query = new Query(sft.getTypeName, filter, Array("track", "dtg", "geom"))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "track")
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 10)

      foreach(ds.getQueryPlan(query)) { qp =>
        qp must beAnInstanceOf[CoprocessorPlan]
        qp.scans.head.getFamilies.map(Bytes.toString) mustEqual Array("A")
      }

      val arrows = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      arrows.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(new RootAllocator(Long.MaxValue)) { allocator =>
        WithClose(SimpleFeatureArrowFileReader.streaming(in)(allocator)) { reader =>
          val results = SelfClosingIterator(reader.features()).map { f =>
            // round the points, as precision is lost due to the arrow encoding
            val attributes = f.getAttributes.asScala.collect {
              case p: Point => s"POINT (${Math.round(p.getX * 10) / 10d} ${Math.round(p.getY * 10) / 10d})"
              case a => a
            }
            ScalaSimpleFeature.create(f.getFeatureType, f.getID, attributes: _*)
          }.toList
          results must containTheSameElementsAs((4 to 8).toFeatures(query.getPropertyNames))
        }
      }
    }
    "work with bin queries" in {
      val filter = ECQL.toFilter("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z " +
          "and bbox(geom,45.4,54.9,45.8,55.1)")

      val query = new Query(sft.getTypeName, filter)
      query.getHints.put(BIN_TRACK, "track")
      query.getHints.put(BIN_BATCH_SIZE, 1000)

      foreach(ds.getQueryPlan(query)) { qp =>
        qp must beAnInstanceOf[CoprocessorPlan]
        qp.scans.head.getFamilies.map(Bytes.toString) mustEqual Array("A")
      }

      val bytes = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      bytes.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))

      val expected = (4 to 8).toFeatures(null).map { f =>
        val track = f.getAttribute("track").hashCode
        val lat = f.getAttribute("geom").asInstanceOf[Point].getY.toFloat
        val lon = f.getAttribute("geom").asInstanceOf[Point].getX.toFloat
        val dtg = f.getAttribute("dtg").asInstanceOf[Date].getTime
        EncodedValues(track, lat, lon, dtg, -1L)
      }

      val bins = out.toByteArray.grouped(16).map(BinaryOutputEncoder.decode).toList
      bins must containTheSameElementsAs(expected)
    }
    "work with density queries" in {
      val filter = ECQL.toFilter("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z " +
          "and bbox(geom,45.4,54.9,45.8,55.1)")
      val envelope = filter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]

      val query = new Query(sft.getTypeName, filter)
      query.getHints.put(QueryHints.LOOSE_BBOX, false)
      query.getHints.put(QueryHints.DENSITY_BBOX, new ReferencedEnvelope(envelope, DefaultGeographicCRS.WGS84))
      query.getHints.put(QueryHints.DENSITY_WIDTH, 640)
      query.getHints.put(QueryHints.DENSITY_HEIGHT, 480)

      foreach(ds.getQueryPlan(query)) { qp =>
        qp must beAnInstanceOf[CoprocessorPlan]
        qp.scans.head.getFamilies.map(Bytes.toString) mustEqual Array("A")
      }

      val decode = DensityScan.decodeResult(envelope, 640, 480)
      val grid = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).flatMap(decode).toList
      grid.map(_._3).sum mustEqual 5 // 5 results
    }
    "work with stats queries" in {
      val filter = ECQL.toFilter("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-01T08:30:00.000Z " +
          "and bbox(geom,45.4,54.9,45.8,55.1)")

      val query = new Query(sft.getTypeName, filter)
      query.getHints.put(QueryHints.STATS_STRING, "MinMax(track)")
      query.getHints.put(QueryHints.ENCODE_STATS, true)

      foreach(ds.getQueryPlan(query)) { qp =>
        qp must beAnInstanceOf[CoprocessorPlan]
        qp.scans.head.getFamilies.map(Bytes.toString) mustEqual Array("A")
      }

      def decode(sf: SimpleFeature): Stat = StatsScan.decodeStat(sft)(sf.getAttribute(0).asInstanceOf[String])
      val stats = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).map(decode).toList
      stats must haveLength(1) // stats will always return a single feature
      val stat = stats.head
      stat must beAnInstanceOf[MinMax[String]]
      stat.asInstanceOf[MinMax[String]].min mustEqual "track-0"
      stat.asInstanceOf[MinMax[String]].max mustEqual "track-2"
    }
  }

  step {
    logger.info("Cleaning up HBase column groups test")
    ds.dispose()
  }
}
