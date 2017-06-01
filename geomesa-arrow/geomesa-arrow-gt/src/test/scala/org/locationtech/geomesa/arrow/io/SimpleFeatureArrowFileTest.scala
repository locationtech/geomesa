/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.apache.arrow.memory.RootAllocator
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureArrowFileTest extends Specification {

  implicit val allocator = new RootAllocator(Long.MaxValue)

  val fileCount = new AtomicInteger(0)

  val sft = SimpleFeatureTypes.createType("test", "name:String,foo:String,dtg:Date,*geom:Point:srid=4326")

  val features0 = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"foo${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }
  val features1 = (10 until 20).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"foo${i % 3}", s"2017-03-15T00:$i:00.000Z", s"POINT (4${i -10} 5${i -10})")
  }

  // note: we clone the features before comparing them as they aren't valid once 'next' is called again,
  // and specs doesn't have methods for comparing iterators

  "SimpleFeatureArrowFiles" should {
    "write and read just a schema" >> {
      withTestFile("empty") { file =>
        new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file), encoding = SimpleFeatureEncoding.max(true)).close()
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          reader.sft mustEqual sft
          reader.features().toSeq must beEmpty
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          reader.sft mustEqual sft
          reader.features().toSeq must beEmpty
        }
      }
    }
    "write and read and filter values" >> {
      withTestFile("simple") { file =>
        WithClose(new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file), encoding = SimpleFeatureEncoding.max(true))) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual
                Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9))
          }
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'")))(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual
            Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9)))
        }
      }
    }
    "optimize queries for sorted files" >> {
      withTestFile("sorted") { file =>
        WithClose(new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file), encoding = SimpleFeatureEncoding.max(true), sort = Some(("dtg", false)))) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("dtg > '2017-03-15T00:12:01.000Z'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual features1.drop(3)
          }
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("dtg > '2017-03-15T00:12:01.000Z'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual features1.drop(3)
          }
          // test second invocation
          WithClose(reader.features(ECQL.toFilter("dtg > '2017-03-15T00:12:01.000Z'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual features1.drop(3)
          }
        }
      }
    }
    "write and read multiple logical files in one" >> {
      withTestFile("multi-files") { file =>
        WithClose(new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file), encoding = SimpleFeatureEncoding.max(true))) { writer =>
          features0.foreach(writer.add)
        }
        WithClose(new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file, true), encoding = SimpleFeatureEncoding.max(true))) { writer =>
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual
                Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9))
          }
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'")))(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual
            Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9))
          )
        }
      }
    }
    "write and read dictionary encoded values" >> {
      val dictionaries = Map("foo:String" -> ArrowDictionary.create(Seq("foo0", "foo1", "foo2")))
      withTestFile("dictionary") { file =>
        WithClose(new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file), dictionaries, SimpleFeatureEncoding.max(true))) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
      }
    }
    "write and read dictionary encoded values with defaults" >> {
      val dictionaries = Map("foo" -> ArrowDictionary.create(Seq("foo0", "foo1")))
      withTestFile("dictionary-defaults") { file =>
        WithClose(new SimpleFeatureArrowFileWriter(sft, new FileOutputStream(file), dictionaries, SimpleFeatureEncoding.max(true))) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        val expected = features0 ++ features1.map {
          case f if f.getAttribute("foo") != "foo2" => f
          case f =>
            val attributes = f.getAttributes.toArray
            attributes.update(1, "[other]")
            ScalaSimpleFeature.create(sft, f.getID, attributes: _*)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual expected)
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual expected)
        }
      }
    }
  }

  def withTestFile[T](name: String)(fn: (File) => T): T = {
    val file = Files.createTempFile(s"gm-arrow-file-test-${fileCount.getAndIncrement()}-", "arrow").toFile
    try { fn(file) } finally {
      // note: uncomment to re-create test data for arrow datastore
      // val copy = new File(s"geomesa-arrow/geomesa-arrow-datastore/src/test/resources/data/$name.arrow")
      // org.apache.commons.io.FileUtils.copyFile(file, copy)
      if (!file.delete()) {
        file.deleteOnExit()
      }
    }
  }

  step {
    allocator.close()
  }
}
