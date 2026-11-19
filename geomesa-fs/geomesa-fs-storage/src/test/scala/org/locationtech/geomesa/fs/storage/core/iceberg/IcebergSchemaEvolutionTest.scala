/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.parquet.hadoop.ParquetFileReader
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorageTest.{IcebergRestContainer, SeaweedFsContainer}
import org.locationtech.geomesa.fs.storage.core.StorageCatalog.CatalogUpdate
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.core.parquet.BloomFilterConfig
import org.locationtech.geomesa.fs.storage.core.parquet.s3.S3InputFile
import org.locationtech.geomesa.fs.storage.core.schemes.{DateTimeScheme, Z2Scheme}
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, StorageCatalog}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.Network

import java.net.URI
import java.time.temporal.ChronoUnit
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

class IcebergSchemaEvolutionTest extends SpecificationWithJUnit with BeforeAfterAll {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

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
        catalog.update(sft.getTypeName).columns().add(SimpleFeatureTypes.createDescriptor("height:Float")).apply()
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
        catalog.update(sft.getTypeName).columns().remove("age").apply()
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

    "fail when removing a partitioned column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().remove("geom") must throwAn[org.apache.iceberg.exceptions.ValidationException]
      }
    }

    "rename a column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().rename("name", "firstName").apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.getDescriptor("name") must beNull
          storage.sft.indexOf("firstName") mustEqual 0
          storage.sft.getDescriptor("firstName").getType.getBinding mustEqual classOf[String]
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.copy(storage.sft, orig) // copies features by index instead of name
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.copy(storage.sft, sf))
            }
            val updatedFilter = filter.replaceAllLiterally("name =", "firstName =")
            foreach(transformsList) { transform =>
              val updatedTransform = Option(transform).map(_.map(_.replace("name", "firstName"))).orNull
              testQuery(storage, storage.sft, updatedFilter, updatedTransform, retyped)
            }
          }
        }
      }
    }

    "rename a geometry column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().rename("geom", "location").apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.getDescriptor("geom") must beNull
          storage.sft.indexOf("location") mustEqual 3
          storage.sft.getDescriptor("location").getType.getBinding mustEqual classOf[Point]
          storage.sft.getGeometryDescriptor.getLocalName mustEqual "location"
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.copy(storage.sft, orig)
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.copy(storage.sft, sf))
            }
            val updatedFilter = filter.replaceAllLiterally("geom", "location")
            foreach(transformsList) { transform =>
              val updatedTransform = Option(transform).map(_.map(_.replace("geom", "location"))).orNull
              testQuery(storage, storage.sft, updatedFilter, updatedTransform, retyped)
            }
          }
        }
      }
    }

    "rename the default date column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().rename("dtg", "ts").apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeCount mustEqual sft.getAttributeCount
          storage.sft.getDescriptor("dtg") must beNull
          storage.sft.indexOf("ts") mustEqual 4
          storage.sft.getDescriptor("ts").getType.getBinding mustEqual classOf[Date]
          storage.sft.getDtgField must beSome("ts")
          val newFeatures = features.map { orig =>
            val sf = ScalaSimpleFeature.copy(storage.sft, orig)
            sf.setId(s"1_${sf.getID}")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++ Seq(ScalaSimpleFeature.copy(storage.sft, sf))
            }
            val updatedFilter = filter.replaceAllLiterally("dtg", "ts")
            foreach(transformsList) { transform =>
              val updatedTransform = Option(transform).map(_.map(_.replace("dtg", "ts"))).orNull
              testQuery(storage, storage.sft, updatedFilter, updatedTransform, retyped)
            }
          }
        }
      }
    }

    "reorder a column" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).columns().reorder("name", 1).apply()
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
        catalog.update(sft.getTypeName).columns().reorder("name", sft.getAttributeCount - 1).apply()
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
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("age:Long")).apply()
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
          """{"name":"properties","type":"record","fields":[{"name":"name","type":"string"},""" +
            """{"name":"nested","type":{"name":"kv","type":"record","fields":[{"name":"key","type":"string"}]}}]}"""
        val descriptor = SimpleFeatureTypes.createDescriptor(s"json:String:json=true:json-schema='$updatedType'")
        catalog.update(sft.getTypeName).columns().update(descriptor).apply()
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
            sf.setAttribute("json", s"""{"name":"${i}name","nested":{"key":"k$i"}}""")
            sf
          }
          WithClose(storage.getMultiPartitionWriter())(writer => newFeatures.foreach(writer.write))
          foreach(queryList) { case (filter, expected) =>
            val retyped = expected.flatMap { sf =>
              newFeatures.find(_.getID == s"1_${sf.getID}") ++
                Seq(ScalaSimpleFeature.retype(storage.sft, sf)).map { retyped =>
                  retyped.setAttribute("json", retyped.getAttribute("json").toString.replaceAll(""""count":\d+,""", ""))
                  retyped
                }
            }
            foreach(Seq(null, Array("geom", "dtg", "json"))) { transform =>
              testQuery(storage, storage.sft, filter, transform, retyped)
            }
          }
        }
      }
    }

    "update attribute options without changing columns" in {
      withCatalog { catalog =>
        val before = WithClose(catalog.load(sft.getTypeName))(_.table.schema())
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("name:String:index=true")).apply()
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("*geom:Point:srid=4326")).apply()
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("dtg:Date:foo=bar")).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before) must beTrue
          storage.sft.getDescriptor("name").getUserData.get("index") mustEqual "true"
          storage.sft.getDescriptor("dtg").getUserData.get("foo") mustEqual "bar"
          storage.sft.getGeometryDescriptor.getLocalName mustEqual "geom"
          testQuery(storage, storage.sft, "INCLUDE", null, features)
        }
      }
    }

    "update list and map columns" in {
      val sft = SimpleFeatureTypes.createType("collections", "names:List[String],counts:Map[String,Int],dtg:Date,*geom:Point:srid=4326")
      WithClose(StorageCatalog(newPath())) { catalog =>
        WithClose(catalog.create(sft, Seq("day"))) { _ => }
        val before = WithClose(catalog.load(sft.getTypeName))(_.table.schema())
        // no-op updates of list/map types shouldn't fail, even though the field ids won't match
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("names:List[String]:foo=bar")).apply()
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("counts:Map[String,Int]")).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before) must beTrue
          storage.sft.getDescriptor("names").getUserData.get("foo") mustEqual "bar"
        }
        // widen the element and value types
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("counts:Map[String,Long]")).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getDescriptor("counts").getMapTypes() mustEqual (classOf[String], classOf[java.lang.Long])
        }
        val beforeInvalid = WithClose(catalog.load(sft.getTypeName))(s => (s.table.schema(), SimpleFeatureTypes.encodeType(s.sft)))
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("counts:Map[Int,Long]")) must
          throwAn[UnsupportedOperationException]
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("counts:Map[String,Int]")) must
          throwAn[UnsupportedOperationException]
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("names:List[Int]")) must
          throwAn[UnsupportedOperationException]
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(beforeInvalid._1) must beTrue
          SimpleFeatureTypes.encodeType(storage.sft) mustEqual beforeInvalid._2
        }
      }
    }

    "update nullability of nested fields" in {
      withCatalog { catalog =>
        val countPath = "json.count"
        WithClose(catalog.load(sft.getTypeName))(_.table.schema().findField(countPath).isRequired must beTrue)
        val optionalType =
          """{"type":"record","name":"properties","fields":[{"name":"count","type":["null","int"]},{"name":"name","type":"string"}]}"""
        catalog.update(sft.getTypeName).columns().update(
          SimpleFeatureTypes.createDescriptor(s"json:String:json=true:json-schema='$optionalType'")).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().findField(countPath).isOptional must beTrue
          storage.sft.getDescriptor("json").getJsonSchema() must beSome(optionalType)
        }
        // optional -> required isn't allowed, as existing data may contain nulls
        val before = WithClose(catalog.load(sft.getTypeName))(s => (s.table.schema(), SimpleFeatureTypes.encodeType(s.sft)))
        catalog.update(sft.getTypeName).columns().update(
          SimpleFeatureTypes.createDescriptor(s"json:String:json=true:json-schema='$structuralType'")) must
          throwAn[UnsupportedOperationException]
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before._1) must beTrue
          SimpleFeatureTypes.encodeType(storage.sft) mustEqual before._2
        }
      }
    }

    "update nullability of list elements" in {
      val requiredList = """{"type":"array","items":"string"}"""
      val optionalList = """{"type":"array","items":["null","string"]}"""
      val sft = SimpleFeatureTypes.createType("nullable-lists",
        s"tags:String:json=true:json-schema='$requiredList',dtg:Date,*geom:Point:srid=4326")
      WithClose(StorageCatalog(newPath())) { catalog =>
        WithClose(catalog.create(sft, Seq("day"))) { _ => }
        WithClose(catalog.load(sft.getTypeName))(_.table.schema().findField("tags").`type`().asListType().isElementRequired must beTrue)
        catalog.update(sft.getTypeName).columns().update(
          SimpleFeatureTypes.createDescriptor(s"tags:String:json=true:json-schema='$optionalList'")).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().findField("tags").`type`().asListType().isElementOptional must beTrue
        }
        catalog.update(sft.getTypeName).columns().update(
          SimpleFeatureTypes.createDescriptor(s"tags:String:json=true:json-schema='$requiredList'")) must
          throwAn[UnsupportedOperationException]
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().findField("tags").`type`().asListType().isElementOptional must beTrue
          storage.sft.getDescriptor("tags").getJsonSchema() must beSome(optionalList)
        }
      }
    }

    "reject invalid geometry updates" in {
      withCatalog { catalog =>
        val before = WithClose(catalog.load(sft.getTypeName))(s => (s.table.schema(), SimpleFeatureTypes.encodeType(s.sft)))
        foreach(Seq("name:Point:srid=4326", "geom:String", "geom:LineString:srid=4326", "*geom:Point:srid=4326:encoding=native")) { spec =>
          catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor(spec)) must
            throwAn[UnsupportedOperationException]
        }
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before._1) must beTrue
          SimpleFeatureTypes.encodeType(storage.sft) mustEqual before._2
          testQuery(storage, storage.sft, "INCLUDE", null, features)
        }
        // explicit wkb encoding is fine
        catalog.update(sft.getTypeName).columns().update(SimpleFeatureTypes.createDescriptor("*geom:Point:srid=4326:encoding=wkb")).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before._1) must beTrue
          testQuery(storage, storage.sft, "INCLUDE", null, features)
        }
      }
    }

    "change the partition spec" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).table().setPartitions(Seq("z2:bits=4", "month")).apply()
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

    "apply delete vectors from old specs after changing partitions" in {
      val features = Seq(
        ScalaSimpleFeature.create(sft, "jan", "january", 0, """{"count":0,"name":"jan"}""", "POINT(40 50)", "2014-01-15T00:00:00.000Z"),
        ScalaSimpleFeature.create(sft, "feb", "february", 1, """{"count":1,"name":"feb"}""", "POINT(40 50)", "2014-02-15T00:00:00.000Z"),
      )

      WithClose(StorageCatalog(newPath())) { catalog =>
        WithClose(catalog.create(sft, Seq("year"), None, Seq.empty)) { storage =>
          WithClose(storage.getMultiPartitionWriter()) { writer =>
            features.foreach(writer.write)
          }
          // verify both features landed in the same file
          storage.metadata.files().scan().toList must haveLength(1)
        }

        catalog.update(sft.getTypeName).table().setPartitions(Seq("month")).apply()

        WithClose(catalog.load(sft.getTypeName)) { storage =>
          WithClose(storage.getUpdateWriter(ECQL.toFilter("IN ('jan', 'feb')"), 1))(writer => writer.foreach(_ => writer.remove()))
          testQuery(storage, storage.sft, "INCLUDE", null, Seq.empty)
          testQuery(storage, storage.sft, "dtg DURING 2014-02-01T00:00:00.000Z/2014-03-01T00:00:00.000Z", null, Seq.empty)
        }
      }
    }

    "add bloom filters" in {
      withCatalog { catalog =>
        catalog.update(sft.getTypeName).table().addBloomFilters(Seq(BloomFilterConfig("age", Some(100), Some(0.05)))).apply()
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
        catalog.update(sft.getTypeName).table().removeBloomFilters(Seq("name")).apply()
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
        catalog.update(sft.getTypeName).metadata().setTargetFileSize(Some(1000000)).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sizer.targetSize must beSome(1000000)
        }
        catalog.update(sft.getTypeName).metadata().setTargetFileSize(None).apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sizer.targetSize must beNone
        }
      }
    }

    "apply multiple updates in a single transaction" in {
      withCatalog { catalog =>
        def metadataVersions(storage: FileSystemStorage): Int =
          storage.table.asInstanceOf[HasTableOperations].operations().current().previousFiles().size()
        val before = WithClose(catalog.load(sft.getTypeName))(s => (s.table.schema(), SimpleFeatureTypes.encodeType(s.sft), metadataVersions(s)))
        val update = catalog.update(sft.getTypeName)
        update.columns().add(SimpleFeatureTypes.createDescriptor("height:Float"))
          .columns().rename("name", "firstName")
          // note: this depends on the column added above
          .columns().reorder("height", 0)
          .columns().update(SimpleFeatureTypes.createDescriptor("age:Long"))
          .table().setPartitions(Seq("month"))
          .metadata().setTargetFileSize(Some(1000000))
          .metadata().setUserData("foo", Some("bar"))
        // nothing is persisted until the update is applied
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before._1) must beTrue
          SimpleFeatureTypes.encodeType(storage.sft) mustEqual before._2
          metadataVersions(storage) mustEqual before._3
        }
        update.apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          metadataVersions(storage) mustEqual before._3 + 1
          storage.sft.getAttributeDescriptors.asScala.map(_.getLocalName).toSeq mustEqual
            Seq("height", "firstName", "age", "json", "geom", "dtg")
          storage.sft.getDescriptor("age").getType.getBinding mustEqual classOf[java.lang.Long]
          storage.schemes mustEqual Seq(DateTimeScheme("dtg", storage.sft.indexOf("dtg"), ChronoUnit.MONTHS))
          storage.sizer.targetSize must beSome(1000000)
          storage.sft.getUserData.get("foo") mustEqual "bar"
          val expected = features.map { orig =>
            val sf = new ScalaSimpleFeature(storage.sft, orig.getID)
            sf.setAttribute("firstName", orig.getAttribute("name"))
            Seq("age", "json", "geom", "dtg").foreach(a => sf.setAttribute(a, orig.getAttribute(a)))
            sf.getUserData.putAll(orig.getUserData)
            sf
          }
          testQuery(storage, storage.sft, "INCLUDE", null, expected)
        }
        update.apply() must throwAn[IllegalStateException]
      }
    }

    "validate updates before they are applied" in {
      withCatalog { catalog =>
        val before = WithClose(catalog.load(sft.getTypeName))(s => (s.table.schema(), SimpleFeatureTypes.encodeType(s.sft)))
        foreach(Seq[CatalogUpdate => Any](
          _.columns().add(SimpleFeatureTypes.createDescriptor("name:String")),
          _.columns().update(SimpleFeatureTypes.createDescriptor("age:String")),
          _.columns().update(SimpleFeatureTypes.createDescriptor("foo:String")),
          _.columns().rename("foo", "bar"),
          _.columns().rename("name", "age"),
          _.columns().remove("foo"),
          _.columns().remove("geom"),
          _.columns().reorder("name", 10),
          _.table().setPartitions(Seq("foo")),
          _.table().rename(sft.getTypeName),
        )) { invalid =>
          val update = catalog.update(sft.getTypeName).columns().add(SimpleFeatureTypes.createDescriptor("height:Float"))
          invalid(update) must throwAn[Exception]
        }
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before._1) must beTrue
          SimpleFeatureTypes.encodeType(storage.sft) mustEqual before._2
          testQuery(storage, storage.sft, "INCLUDE", null, features)
        }
        // an invalid change is not staged, and the update can still be used
        val update = catalog.update(sft.getTypeName).columns().add(SimpleFeatureTypes.createDescriptor("height:Float"))
        update.columns().rename("name", "age") must throwAn[Exception]
        update.columns().rename("name", "firstName").apply()
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.sft.getAttributeDescriptors.asScala.map(_.getLocalName).toSeq mustEqual
            Seq("firstName", "age", "json", "geom", "dtg", "height")
        }
      }
    }

    "not allow an update to be applied more than once" in {
      withCatalog { catalog =>
        val update = catalog.update(sft.getTypeName).columns().remove("age")
        // make a conflicting change after the update has been validated, so that the apply fails
        catalog.update(sft.getTypeName).columns().remove("age").apply()
        val before = WithClose(catalog.load(sft.getTypeName))(s => (s.table.schema(), SimpleFeatureTypes.encodeType(s.sft)))
        update.apply() must throwAn[IllegalArgumentException]
        // the update can't be re-tried, even though it failed
        update.apply() must throwAn[IllegalStateException]
        WithClose(catalog.load(sft.getTypeName)) { storage =>
          storage.table.schema().sameSchema(before._1) must beTrue
          SimpleFeatureTypes.encodeType(storage.sft) mustEqual before._2
        }
        // a successful update can't be re-applied either
        val success = catalog.update(sft.getTypeName).columns().add(SimpleFeatureTypes.createDescriptor("height:Float"))
        success.apply()
        success.apply() must throwAn[IllegalStateException]
      }
    }

    "rename a table" in {
      withCatalog { catalog =>
        val renamed = "renamed-test"
        catalog.update(sft.getTypeName).table().rename(renamed).apply()
        catalog.getTypeNames must containTheSameElementsAs(Seq(renamed))
        WithClose(catalog.load(sft.getTypeName))(_ => ()) must throwA[NoSuchTableException]
        WithClose(catalog.load(renamed)) { storage =>
          storage.sft.getTypeName mustEqual renamed
          foreach(queryList) { case (filter, expected) =>
            foreach(transformsList) { transform =>
              testQuery(storage, storage.sft, filter, transform, expected)
            }
          }
        }
      }
    }
  }
}
