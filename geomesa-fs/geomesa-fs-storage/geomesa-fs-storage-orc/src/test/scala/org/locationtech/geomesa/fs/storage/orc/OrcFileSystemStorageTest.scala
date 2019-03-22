/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import java.nio.file.Files
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionMetadata
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, FileSystemStorage, Metadata, NamedOptions}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory
import org.locationtech.geomesa.fs.storage.common.partitions.DateTimeScheme
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OrcFileSystemStorageTest extends Specification with LazyLogging {

  val config = new Configuration()

  // 8 bits resolution creates 3 partitions with our test data
  val scheme = NamedOptions("z2-8bits")

  "OrcFileSystemWriter" should {
    "read and write features" in {
      val sft = SimpleFeatureTypes.createType("orc-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

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
              .create(context, Map.empty, Metadata(sft, "orc", scheme, leafStorage = true))
        val storage = new OrcFileSystemStorageFactory().apply(context, metadata)

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
        loaded.foreach(_.reload()) // ensure state is loaded
        loaded must beSome
        testQuery(new OrcFileSystemStorageFactory().apply(context, loaded.get), sft)("INCLUDE", null, features)
      }
    }

    "read and write complex features" in {
      val sft = SimpleFeatureTypes.createType("orc-test-complex",
        "name:String,age:Int,time:Long,height:Float,weight:Double,bool:Boolean," +
            "uuid:UUID,bytes:Bytes,list:List[Int],map:Map[String,Long]," +
            "line:LineString,mpt:MultiPoint,poly:Polygon,mline:MultiLineString,mpoly:MultiPolygon," +
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
        sf.setAttribute("mpoly", s"MULTIPOLYGON(((-1 0, 0 $i, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))")
        sf.setAttribute("dtg", f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute("geom", s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext(FileContext.getFileContext(dir.toUri), config, dir)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "orc", scheme, leafStorage = true))
        val storage = new OrcFileSystemStorageFactory().apply(context, metadata)

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
    "transition old metadata files" in {
      withTestDir { dir =>
        val context = FileSystemContext(FileContext.getFileContext(dir.toUri), config, dir)
        val meta = new Path(dir, "metadata.json")
        context.fc.util.copy(new Path(getClass.getClassLoader.getResource("metadata-old.json").toURI), meta)
        context.fc.util.exists(meta) must beTrue
        PathCache.invalidate(context.fc, meta)

        val metadata = new FileBasedMetadataFactory().load(context)
        metadata.foreach(_.reload()) // ensure state is loaded
        metadata must beSome
        val storage = new OrcFileSystemStorageFactory().apply(context, metadata.get)

        storage.metadata.encoding mustEqual "orc"
        storage.metadata.sft.getTypeName mustEqual "example-csv"
        storage.metadata.scheme must beAnInstanceOf[DateTimeScheme]
        storage.getPartitions must containTheSameElementsAs(
          Seq(
            PartitionMetadata("2015/05/06", Seq("06_Wb48cb7293793447480c0885f3f4bb56a.orc"), None, 0L),
            PartitionMetadata("2015/06/07", Seq("07_W25d311113f0b4bad819f209f00a58173.orc"), None, 0L),
            PartitionMetadata("2015/10/23", Seq("23_Weedeb59bad0d4521b2ae46189eac4a4d.orc"), None, 0L)
          )
        )
      }
    }
  }

  def withTestDir[R](code: Path => R): R = {
    val file = new Path(Files.createTempDirectory("gm-orc-test").toUri)
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
