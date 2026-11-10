/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.parquet.hadoop.ParquetFileReader
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorageTest.{IcebergRestContainer, SeaweedFsContainer}
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.core.parquet.BloomFilterConfig
import org.locationtech.geomesa.fs.storage.core.parquet.s3.S3InputFile
import org.locationtech.geomesa.fs.storage.core.schemes.{DateTimeScheme, Z2Scheme}
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, StorageCatalog}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.Network

import java.net.URI
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicInteger

class IcebergSchemaEvolutionTest extends SpecificationWithJUnit with BeforeAfterAll {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import scala.collection.JavaConverters._

  private val paths = new AtomicInteger()

  private val network = Network.newNetwork()

  private val s3 = new SeaweedFsContainer().withNetwork(network)

  private val iceberg = new IcebergRestContainer().withNetwork(network)

  private lazy val s3Conf = Map(
    "fs.s3.region" -> "us-east-1",
    "fs.s3.endpoint" -> s3.getS3URL,
    "fs.s3.access-key-id" -> "admin",
    "fs.s3.secret-access-key" -> "admin",
    "fs.s3.force-path-style" -> "true",
  )

  private val structuralType =
    """{"type":"record","name":"properties","fields":[{"name":"count","type":"int"},{"name":"name","type":"string"}]}"""

  private val sft =
    SimpleFeatureTypes.createType("parquet-test",
      s"name:String,age:Int,json:String:json=true:json-schema='$structuralType',*geom:Point:srid=4326,dtg:Date")

  private val features = Seq.tabulate(10) { i =>
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf.setAttribute(0, s"name$i")
    sf.setAttribute(1, s"$i")
    sf.setAttribute(2, s"""{"count":$i,"name":"${i}name"}""")
    sf.setAttribute(3, s"POINT(4$i 5$i)")
    sf.setAttribute(4, f"2014-01-${i + 1}%02dT00:00:01.000Z")
    sf
  }

  private val hiddenFeatures = features.take(1).map(ScalaSimpleFeature.copy).map { sf =>
    sf.setId("-1")
    sf.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "admin")
    sf
  }

  private val transformsList = Seq(null, Array("name", "dtg", "geom"), Array("geom", "dtg"))

  private val queryList = Map(
    "INCLUDE" -> features,
    "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z" -> features.dropRight(2),
    "name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z" -> features.slice(5, 6),
    "name = 'name5'" -> features.slice(5, 6),
    "age < 5" -> features.take(5),
  )

  override def beforeAll(): Unit = {
    s3.start()
    iceberg.start()
  }

  override def afterAll(): Unit = {
    iceberg.stop()
    s3.stop()
    network.close()
  }

  // make a valid, unique namespace for each test
  def newPath(): Map[String, String] = {
    val path = f"${paths.getAndIncrement()}%03d"
    s3Conf ++ Map(
      "type" -> "rest",
      "uri" -> s"http://${iceberg.getHost}:${iceberg.getFirstMappedPort}/",
      "iceberg.namespace" -> path,
    )
  }

  def withCatalog[T](fn: StorageCatalog => T): T = {
    WithClose(StorageCatalog(newPath())) { catalog =>
      WithClose(catalog.create(sft, Seq("z2:bits=8", "day"), None, Seq(BloomFilterConfig("name")))) { storage =>
        WithClose(storage.getMultiPartitionWriter()) { writer =>
          features.foreach(writer.write)
          hiddenFeatures.foreach(writer.write) // validate that vis still work correctly
        }
      }
      fn(catalog)
    }
  }

  def testQuery(
      storage: FileSystemStorage,
      sft: SimpleFeatureType,
      filter: String,
      transforms: Array[String],
      results: Seq[SimpleFeature]): MatchResult[Any] = {
    val query = new Query(sft.getTypeName, ECQL.toFilter(filter), transforms: _*)
    val features = CloseableIterator(storage.getReader(query, 1)).map(ScalaSimpleFeature.copy).toList
    val attributes = Option(transforms).getOrElse(sft.getAttributeDescriptors.asScala.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
      }
      SecurityUtils.getVisibility(feature) mustEqual SecurityUtils.getVisibility(results.find(_.getID == feature.getID).get)
    }
  }

  "IcebergSchemaEvolution" should {
    "add a column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().add(SimpleFeatureTypes.createDescriptor("height:Float"))
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount + 1
          storage.sft.getDescriptor(sft.getAttributeCount).getLocalName mustEqual "height"
          storage.sft.getDescriptor(sft.getAttributeCount).getType.getBinding mustEqual classOf[java.lang.Float]
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.retype(storage.sft, orig).asInstanceOf[ScalaSimpleFeature]
            sf.setAttribute("height", s"${orig.getAttribute("age")}.${orig.getAttribute("age")}")
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.retype(storage.sft, sf))
            }
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
          testQuery(storage, storage.sft, "height > 5", null, newFeatures.drop(5))
        }
      }
    }

    "remove a column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().remove("age")
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount - 1
          storage.sft.getDescriptor("age") must beNull
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.retype(storage.sft, orig).asInstanceOf[ScalaSimpleFeature]
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            if (filter.contains("age")) {
              testQuery(storage, storage.sft, filter, null, Seq.empty) must throwAn[IllegalArgumentException]
            } else {
              val retyped = expected.flatMap { sf =>
                newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.retype(storage.sft, sf))
              }
              foreach(transformsList) { transform =>
                testQuery(storage, storage.sft, filter, transform, retyped)
              }
            }
          }
        }
      }
    }

    "rename a column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().rename("name", "firstName")
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.getDescriptor("name") must beNull
          storage.sft.indexOf("firstName") mustEqual 0
          storage.sft.getDescriptor("firstName").getType.getBinding mustEqual classOf[String]
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.retype(storage.sft, orig).asInstanceOf[ScalaSimpleFeature]
            sf.setAttribute("firstName", orig.getAttribute("name"))
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++
                Seq(ScalaSimpleFeature.retype(storage.sft, sf)).map { r => r.setAttribute("firstName", sf.getAttribute("name")); r }
            }
            foreach(transformsList) { transform =>
              val updatedFilter = filter.replaceAllLiterally("name =", "firstName =")
              val updatedTransform = Option(transform).map(_.map(_.replace("name", "firstName"))).orNull
              testQuery(storage, storage.sft, updatedFilter, updatedTransform, retyped)
            }
          }
        }
      }
    }

    "reorder a column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().reorder("name", 1)
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.indexOf("name") mustEqual 1
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.retype(storage.sft, orig).asInstanceOf[ScalaSimpleFeature]
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.retype(storage.sft, sf))
            }
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "reorder a column to last" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().reorder("name", sft.getAttributeCount - 1)
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.indexOf("name") mustEqual sft.getAttributeCount - 1
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.retype(storage.sft, orig).asInstanceOf[ScalaSimpleFeature]
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.retype(storage.sft, sf))
            }
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "widen a column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("age:Long"))
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.indexOf("age") mustEqual sft.indexOf("age")
          storage.sft.getDescriptor("age").getType.getBinding mustEqual classOf[java.lang.Long]
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.retype(storage.sft, orig).asInstanceOf[ScalaSimpleFeature]
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.retype(storage.sft, sf))
            }
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "update a structural type" in {
      withCatalog { catalog =>
        val updatedType =
          """{"name":"properties","type":"record","fields":[{"name":"count","type":"int"},{"name":"name","type":"string"},""" +
            """{"name":"nested","type":{"name":"kv","type":"record","fields":[{"name":"key","type":"string"}]}}]}"""
        val descriptor = SimpleFeatureTypes.createDescriptor(s"json:String:json=true:json-schema='$updatedType'")
        catalog.update(sft.getTypeName).columns().update(descriptor)
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.indexOf("json") mustEqual sft.indexOf("json")
          storage.sft.getDescriptor("json").getType.getBinding mustEqual classOf[java.lang.String]
          storage.sft.getDescriptor("json").isJson() must beTrue
          storage.sft.getDescriptor("json").getJsonSchema() must beSome(updatedType)
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.retype(storage.sft, orig).asInstanceOf[ScalaSimpleFeature]
            sf.setId(s"1_${sf.getID}")
            val i = sf.getAttribute("age")
            sf.setAttribute("json", s"""{"count":$i,"name":"${i}name","nested":{"key":"k$i"}}""")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.retype(storage.sft, sf))
            }
            foreach(Seq(null, Array("geom", "dtg", "json"))) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "change the partition spec" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).indices().setPartitions(Seq("z2:bits=4", "month"))
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          // note: original order isn't preserved, schemes are sorted by name
          storage.schemes mustEqual Seq(DateTimeScheme("dtg", 4, ChronoUnit.MONTHS), Z2Scheme("geom", 3, 4))
          storage.schema.schema.findColumnName(storage.table.spec().fields().get(0).sourceId()) mustEqual "dtg"
          storage.table.spec().fields().get(0).transform().toString mustEqual "month"
          storage.schema.schema.findColumnName(storage.table.spec().fields().get(1).sourceId()) mustEqual "__geom_z2__"
          storage.table.spec().fields().get(1).transform().toString mustEqual "truncate[1]"
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.copy(orig)
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap(sf => newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(sf))
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "add bloom filters" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).indices().addBloomFilters(Seq(BloomFilterConfig("age", Some(100), Some(0.05))))
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          val oldFiles = storage.metadata.files().scan().map(_.file().location())
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.copy(orig)
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          val newFiles = storage.metadata.files().scan().map(_.file().location()).filterNot(oldFiles.contains)
          val bloomFilters = WithClose(S3ObjectStore(s3Conf)) { fs =>
            (oldFiles ++ newFiles).map { f =>
              WithClose(ParquetFileReader.open(new S3InputFile(fs, URI.create(f)))) { reader =>
                val columns = reader.getFooter.getBlocks.asScala.head.getColumns.asScala
                f -> columns.flatMap(c => Option(reader.readBloomFilter(c)).map(c.getPath.toDotString -> _))
              }
            }.toMap
          }
          // note: parquet may intentionally omit bloom filters when all data pages are dictionary encoded
          oldFiles.count(bloomFilters.apply(_).nonEmpty) must beGreaterThan(0)
          foreach(oldFiles) { f =>
            bloomFilters(f).length must beBetween(0, 1)
            foreach(bloomFilters(f))(_._1 mustEqual "name")
          }
          foreach(newFiles) { f =>
            bloomFilters(f) must haveLength(2)
            bloomFilters(f).map(_._1).sorted mustEqual Seq("age", "name")
          }
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap(sf => newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(sf))
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "remove bloom filters" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).indices().removeBloomFilters(Seq("name"))
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          val oldFiles = storage.metadata.files().scan().map(_.file().location())
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.copy(orig)
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          val newFiles = storage.metadata.files().scan().map(_.file().location()).filterNot(oldFiles.contains)
          val bloomFilters = WithClose(S3ObjectStore(s3Conf)) { fs =>
            (oldFiles ++ newFiles).map { f =>
              WithClose(ParquetFileReader.open(new S3InputFile(fs, URI.create(f)))) { reader =>
                val columns = reader.getFooter.getBlocks.asScala.head.getColumns.asScala
                f -> columns.flatMap(c => Option(reader.readBloomFilter(c)).map(c.getPath.toDotString -> _))
              }
            }.toMap
          }
          // note: parquet may intentionally omit bloom filters when all data pages are dictionary encoded
          oldFiles.count(bloomFilters.apply(_).nonEmpty) must beGreaterThan(0)
          foreach(oldFiles) { f =>
            bloomFilters(f).length must beBetween(0, 1)
            foreach(bloomFilters(f))(_._1 mustEqual "name")
          }
          foreach(newFiles) { f =>
            bloomFilters(f) must beEmpty
          }
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap(sf => newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(sf))
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "update target file size" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).metadata().setTargetFileSize(Some(1000000))
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sizer.targetSize must beSome(1000000)
        }
        catalog.update(sft.getTypeName).metadata().setTargetFileSize(None)
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sizer.targetSize must beNone
        }
      }
    }
  }
}
