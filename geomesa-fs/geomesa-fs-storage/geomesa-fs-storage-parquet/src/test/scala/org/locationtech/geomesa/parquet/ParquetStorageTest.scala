/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.nio.file.Files
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.StorageKeys
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class ParquetStorageTest extends Specification with AllExpectations with LazyLogging {

  sequential

  val config = new Configuration()
  config.set("parquet.compression", "gzip")

  // 8 bits resolution creates 3 partitions with our test data
  val scheme = NamedOptions("z2-8bits")

  "ParquetFileSystemStorage" should {
    "read and write features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext(FileContext.getFileContext(dir.toUri), config, dir)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
        val storage = new ParquetFileSystemStorageFactory().apply(context, metadata)

        storage must not(beNull)

        val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

        features.foreach { f =>
          val partition = storage.metadata.scheme.getPartitionName(f)
          val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
          writer.write(f)
        }

        writers.foreach(_._2.close())

        logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

        val partitions = storage.getPartitions.map(_.name)
        partitions must haveLength(writers.size)

        val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))

        val doTest = testQuery(storage, sft) _

        foreach(transformsList) { transforms =>
          doTest("INCLUDE", transforms, features)
          doTest("IN('0', '2')", transforms, Seq(features(0), features(2)))
          doTest("bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
          doTest("bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z", transforms, features.drop(2))
          doTest("bbox(geom,42,48,52,62)", transforms, features.drop(2))
          doTest("dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
          doTest("name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.slice(5, 6))
          doTest("name < 'name5'", transforms, features.take(5))
          doTest("name = 'name5'", transforms, features.slice(5, 6))
          doTest("age < 5", transforms, features.take(5))
        }

        // verify we can load an existing storage
        val loaded = new FileBasedMetadataFactory().load(context)
        loaded must beSome
        testQuery(new ParquetFileSystemStorageFactory().apply(context, loaded.get), sft)("INCLUDE", null, features)
      }
    }

    "read and write complex features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test-complex",
        "name:String,age:Int,time:Long,height:Float,weight:Double,bool:Boolean," +
            "uuid:UUID,bytes:Bytes,list:List[Int],map:Map[String,Long]," +
            "line:LineString,mpt:MultiPoint,poly:Polygon,mline:MultiLineString,mpoly:MultiPolygon,g:Geometry," +
            "dtg:Date,*geom:Point:srid=4326")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute("name", s"name$i")
        sf.setAttribute("age", s"$i")
        sf.setAttribute("time", s"$i")
        sf.setAttribute("height", s"$i")
        sf.setAttribute("weight", s"$i")
        sf.setAttribute("bool", Boolean.box(i < 5))
        sf.setAttribute("uuid", UUID.fromString(s"00000000-0000-0000-0000-00000000000$i"))
        sf.setAttribute("bytes", Array.tabulate[Byte](i)(i => i.toByte))
        sf.setAttribute("list", Seq.tabulate[Integer](i)(i => Int.box(i)))
        sf.setAttribute("map", (0 until i).map(i => i.toString -> Long.box(i)).toMap)
        sf.setAttribute("line", s"LINESTRING(0 $i, 2 $i, 8 ${10 - i})")
        sf.setAttribute("mpt", s"MULTIPOINT(0 $i, 2 3)")
        sf.setAttribute("poly",
          if (i == 5) {
            // multipolygon example from wikipedia
            "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))"
          } else {
            s"POLYGON((40 3$i, 42 3$i, 42 2$i, 40 2$i, 40 3$i))"
          }
        )
        sf.setAttribute("mline", s"MULTILINESTRING((0 2, 2 $i, 8 6),(0 $i, 2 $i, 8 ${10 - i}))")
        sf.setAttribute("mpoly", s"MULTIPOLYGON(((-1 0, 0 $i, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6), (-1 5, 2 5, 2 2, -1 2, -1 5)))")
        sf.setAttribute("g", sf.getAttribute(Seq("line", "mpt", "poly", "mline", "mpoly").drop(i % 5).head))
        sf.setAttribute("dtg", f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute("geom", s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext(FileContext.getFileContext(dir.toUri), config, dir)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
        val storage = new ParquetFileSystemStorageFactory().apply(context, metadata)

        storage must not(beNull)

        val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

        features.foreach { f =>
          val partition = storage.metadata.scheme.getPartitionName(f)
          val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
          writer.write(f)
        }

        writers.foreach(_._2.close())

        logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

        val partitions = storage.getPartitions.map(_.name)
        partitions must haveLength(writers.size)

        val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))

        val doTest = testQuery(storage, sft) _

        foreach(transformsList) { transforms =>
          doTest("INCLUDE", transforms, features)
          doTest("IN('0', '2')", transforms, Seq(features(0), features(2)))
          doTest("bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
          doTest("bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z", transforms, features.drop(2))
          doTest("bbox(geom,42,48,52,62)", transforms, features.drop(2))
          doTest("dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
          doTest("name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.slice(5, 6))
          doTest("name < 'name5'", transforms, features.take(5))
          doTest("name = 'name5'", transforms, features.slice(5, 6))
          doTest("age < 5", transforms, features.take(5))
          doTest("age > 5", transforms, features.drop(6))
        }
      }
    }

    "modify and delete features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext(FileContext.getFileContext(dir.toUri), config, dir)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
        val storage = new ParquetFileSystemStorageFactory().apply(context, metadata)

        storage must not(beNull)

        val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

        features.foreach { f =>
          val partition = storage.metadata.scheme.getPartitionName(f)
          val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
          writer.write(f)
        }

        writers.foreach(_._2.close())

        logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

        testQuery(storage, sft)("INCLUDE", null, features)

        val updater = storage.getWriter(Filter.INCLUDE)

        updater.hasNext must beTrue
        while (updater.hasNext) {
          val feature = updater.next
          if (feature.getID == "0") {
            updater.remove()
          } else if (feature.getID == "1") {
            feature.setAttribute(1, "name-updated")
            updater.write()
          }
        }
        updater.close()

        val updates = features.drop(2) :+ {
          val mod = ScalaSimpleFeature.copy(features.drop(1).head)
          mod.setAttribute("name", "name-updated")
          mod
        }

        testQuery(storage, sft)("INCLUDE", null, updates)
      }
    }

    "use custom file observers" in {
      val userData = s"${StorageKeys.ObserversKey}=${classOf[TestObserverFactory].getName}"
      val sft = SimpleFeatureTypes.createType("parquet-test",
        s"*geom:Point:srid=4326,name:String,age:Int,dtg:Date;$userData")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext(FileContext.getFileContext(dir.toUri), config, dir)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
        val storage = new ParquetFileSystemStorageFactory().apply(context, metadata)

        storage must not(beNull)

        val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

        features.foreach { f =>
          val partition = storage.metadata.scheme.getPartitionName(f)
          val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
          writer.write(f)
        }

        TestObserverFactory.observers must haveSize(3) // 3 partitions due to our data and scheme
        forall(TestObserverFactory.observers)(_.closed must beFalse)

        writers.foreach(_._2.close())
        forall(TestObserverFactory.observers)(_.closed must beTrue)
        TestObserverFactory.observers.flatMap(_.features) must containTheSameElementsAs(features)
        TestObserverFactory.observers.clear()

        logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

        val updater = storage.getWriter(Filter.INCLUDE)

        updater.hasNext must beTrue
        while (updater.hasNext) {
          val feature = updater.next
          if (feature.getID == "0") {
            updater.remove()
          } else if (feature.getID == "1") {
            feature.setAttribute(1, "name-updated")
            updater.write()
          }
        }

        TestObserverFactory.observers must haveSize(2) // 2 partitions were updated
        forall(TestObserverFactory.observers)(_.closed must beFalse)

        updater.close()

        forall(TestObserverFactory.observers)(_.closed must beTrue)
        TestObserverFactory.observers.flatMap(_.features) must haveLength(2)
      }
    }

    "read old files" in {
      val url = getClass.getClassLoader.getResource("data/2.3.0/example-csv/")
      url must not(beNull)
      val path = new Path(url.toURI)
      val context = FileSystemContext(FileContext.getFileContext(url.toURI), config, path)
      val metadata = StorageMetadataFactory.load(context).orNull
      metadata must not(beNull)
      val storage = FileSystemStorageFactory(context, metadata)

      val features = SelfClosingIterator(storage.getReader(new Query("example-csv"))).toList.sortBy(_.getID)
      features must haveLength(3)
      features.map(_.getAttribute("name").asInstanceOf[String]) mustEqual Seq("Harry", "Hermione", "Severus")
      features.map(_.getAttribute("geom").toString) mustEqual
          Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
    }
  }

  def withTestDir[R](code: Path => R): R = {
    val file = new Path(Files.createTempDirectory("gm-parquet-test").toUri)
    try { code(file) } finally {
      file.getFileSystem(new Configuration).delete(file, true)
    }
  }

  def testQuery(storage: FileSystemStorage,
      sft: SimpleFeatureType)
      (filter: String,
          transforms: Array[String],
          results: Seq[SimpleFeature]): MatchResult[Any] = {
    import scala.collection.JavaConversions._

    val query = new Query(sft.getTypeName, ECQL.toFilter(filter), transforms)
    val features = {
      val iter = SelfClosingIterator(storage.getReader(query))
      // note: need to copy features in iterator as same object is re-used
      iter.map(ScalaSimpleFeature.copy).toList
    }
    val attributes = Option(transforms).getOrElse(sft.getAttributeDescriptors.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
      }
    }
  }
}
