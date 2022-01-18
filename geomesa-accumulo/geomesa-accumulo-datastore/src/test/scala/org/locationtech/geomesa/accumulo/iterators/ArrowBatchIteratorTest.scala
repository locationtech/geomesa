/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, Closeable}

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.{SingleRowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.LineString
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortOrder
import org.specs2.matcher.MatchResult
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ArrowBatchIteratorTest extends TestWithMultipleSfts with Mockito {

  import scala.collection.JavaConverters._

  sequential

  lazy val pointSft = createNewSchema("name:String:index=join,team:String:index-value=true,age:Int,weight:Int,dtg:Date,*geom:Point:srid=4326")
  lazy val lineSft = createNewSchema("name:String:index=join,team:String:index-value=true,age:Int,weight:Int,dtg:Date,*geom:LineString:srid=4326")
  lazy val listSft = createNewSchema("names:List[String],team:String,dtg:Date,*geom:Point:srid=4326")

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val pointFeatures = (0 until 10).map { i =>
    val name = s"name${i % 2}"
    val team = s"team$i"
    val age = i % 5
    val weight = Option(i % 3).filter(_ != 0).map(Int.box).orNull
    ScalaSimpleFeature.create(pointSft, s"$i", name, team, age, weight, s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  val lineFeatures = (0 until 10).map { i =>
    val name = s"name${i % 2}"
    val team = s"team$i"
    val age = i % 5
    val weight = Option(i % 3).filter(_ != 0).map(Int.box).orNull
    val geom = s"LINESTRING(40 6$i, 40.1 6$i, 40.2 6$i, 40.3 6$i)"
    ScalaSimpleFeature.create(lineSft, s"$i", name, team, age, weight, s"2017-02-03T00:0$i:01.000Z", geom)
  }

  val listFeatures = (0 until 10).map { i =>
    val names = Seq.tabulate(i % 3)(j => s"name0$j").asJava
    val team = s"team$i"
    ScalaSimpleFeature.create(listSft, s"$i", names, team, s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  // hit all major indices
  val filters = Seq(
    "bbox(geom, 38, 59, 42, 70)",
    "bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z",
    s"IN(${pointFeatures.map(_.getID).mkString("'", "', '", "'")})",
    "name IN('name0', 'name1')"
  ).map(ECQL.toFilter)

  addFeatures(pointFeatures)
  addFeatures(lineFeatures)
  addFeatures(listFeatures)

  val sfts = Seq((pointSft, pointFeatures), (lineSft, lineFeatures))

  lazy val localDs =
    DataStoreFinder
        .getDataStore((dsParams ++ Map(AccumuloDataStoreParams.RemoteArrowParam.key -> "false")).asJava)
        .asInstanceOf[AccumuloDataStore]
  lazy val dataStores = Seq(ds, localDs)

  def compare(results: Iterator[SimpleFeature] with Closeable,
              expected: Seq[SimpleFeature],
              transform: Seq[String] = Seq.empty,
              ordered: Boolean = false): MatchResult[Any] = {
    val transformed = if (transform.isEmpty) { expected } else {
      import scala.collection.JavaConversions._
      val tsft = {
        val builder = new SimpleFeatureTypeBuilder
        builder.setName(expected.head.getFeatureType.getTypeName)
        val descriptors = expected.head.getFeatureType.getAttributeDescriptors
        transform.foreach(t => builder.add(descriptors.find(_.getLocalName == t).orNull))
        builder.buildFeatureType()
      }
      expected.map { e =>
        new ScalaSimpleFeature(tsft, e.getID, tsft.getAttributeDescriptors.map(d => e.getAttribute(d.getLocalName)).toArray)
      }
    }
    if (ordered) {
      val features = SelfClosingIterator(results).map(ScalaSimpleFeature.copy).toSeq
      features must haveLength(transformed.length)
      foreach(features.zip(transformed)) { case (f, e) => compare(f, e) }
    } else {
      val features = SelfClosingIterator(results).map(ScalaSimpleFeature.copy).toList
      features must containTheSameElementsAs(transformed,
        (a: SimpleFeature, e: SimpleFeature) => Try(compare(a, e).isSuccess).getOrElse(false))
    }
  }

  def compare(feature: SimpleFeature, expected: SimpleFeature): MatchResult[Any] = {
    feature.getID mustEqual expected.getID
    feature.getAttributeCount mustEqual expected.getAttributeCount
    foreach(0 until feature.getAttributeCount)(i => compareAttributes(feature.getAttribute(i), expected.getAttribute(i)))
  }

  def compareAttributes(attribute: AnyRef, expected: AnyRef): MatchResult[Any] = {
    expected match {
      case els: LineString =>
        // because of our limited precision in arrow queries, points don't exactly match up
        attribute must beAnInstanceOf[LineString]
        val als = attribute.asInstanceOf[LineString]
        als.getNumPoints mustEqual els.getNumPoints
        foreach(0 until als.getNumPoints) { n =>
          als.getCoordinateN(n).x must beCloseTo(els.getCoordinateN(n).x, 0.001)
          als.getCoordinateN(n).y must beCloseTo(els.getCoordinateN(n).y, 0.001)
        }

      case _ => attribute mustEqual expected
    }
  }

  "ArrowBatchIterator" should {
    "return arrow encoded data" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val query = new Query(sft.getTypeName, filter)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            query.getHints.put(QueryHints.ARROW_DOUBLE_PASS, true)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features)
            }
          }
        }
      }
      ok
    }
    "return arrow dictionary encoded data" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val query = new Query(sft.getTypeName, filter)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features)
            }
          }
        }
      }
      ok
    }
    "return arrow dictionary encoded ints" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val query = new Query(sft.getTypeName, filter)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "age")
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features)
            }
          }
        }
      }
      ok
    }
    "return arrow dictionary encoded data with cached data" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          val filter = ECQL.toFilter("name = 'name0'")
          val query = new Query(sft.getTypeName, filter)
          query.getHints.put(QueryHints.ARROW_ENCODE, true)
          query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
          query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          val out = new ByteArrayOutputStream
          results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
          def in() = new ByteArrayInputStream(out.toByteArray)
          WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
            compare(reader.features(), features.filter(filter.evaluate))
            // verify all cached values were used for the dictionary
            reader.dictionaries.map { case (k, v) => (k, v.iterator.toSeq) } mustEqual Map("name" -> Seq("name0", "name1"))
          }
        }
      }
      ok
    }
    "return arrow dictionary encoded data without caching" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          val filter = ECQL.toFilter("name = 'name0'")
          val query = new Query(sft.getTypeName, filter)
          query.getHints.put(QueryHints.ARROW_ENCODE, true)
          query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
          query.getHints.put(QueryHints.ARROW_DICTIONARY_CACHED, java.lang.Boolean.FALSE)
          query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          val out = new ByteArrayOutputStream
          results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
          def in() = new ByteArrayInputStream(out.toByteArray)
          WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
            compare(reader.features(), features.filter(filter.evaluate))
            // verify only exact values were used for the dictionary
            reader.dictionaries.map { case (k, v) => (k, v.iterator.toSeq) } mustEqual Map("name" -> Seq("name0"))
          }
        }
      }
      ok
    }
    "return arrow dictionary encoded data without caching and with z-values" in {
      dataStores.foreach { ds =>
        val filter = ECQL.toFilter("bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z")
        val query = new Query(pointSft.getTypeName, filter)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
        query.getHints.put(QueryHints.ARROW_DICTIONARY_CACHED, java.lang.Boolean.FALSE)
        foreach(ds.getQueryPlan(query)) { plan =>
          val expected = if (ds.config.remote.arrow) {
            Seq(classOf[Z3Iterator], classOf[ArrowIterator])
          } else {
            Seq(classOf[Z3Iterator])
          }
          plan.iterators.map(_.getIteratorClass) must containTheSameElementsAs(expected.map(_.getName))
        }
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          compare(reader.features(), pointFeatures.filter(filter.evaluate))
        }
      }
      ok
    }
    "return arrow dictionary encoded data with provided dictionaries" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val query = new Query(sft.getTypeName, filter)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
            query.getHints.put(QueryHints.ARROW_DICTIONARY_VALUES, "name,name0")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              val expected = features.map {
                case f if f.getAttribute(0) != "name1" => f
                case f =>
                  val e = ScalaSimpleFeature.copy(sft, f)
                  e.setAttribute(0, "[other]")
                  e
              }
              compare(reader.features(), expected)
            }
          }
        }
      }
      ok
    }
    "return arrow encoded projections" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          foreach(filters.take(1)) { filter =>
            foreach(Seq(Array("dtg", "geom")/*, Array("name", "geom")*/)) { transform =>
              val query = new Query(sft.getTypeName, filter, transform)
              query.getHints.put(QueryHints.ARROW_ENCODE, true)
              query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
              val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
              val out = new ByteArrayOutputStream
              results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
              def in() = new ByteArrayInputStream(out.toByteArray)
              WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
                compare(reader.features(), features, transform.toSeq)
              }
            }
          }
        }
      }
      ok
    }
    "return sorted batches" in {
      // TODO figure out how to test multiple batches (client side merge)
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val query = new Query(sft.getTypeName, filter)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features, ordered = true)
            }
          }
        }
      }
      ok
    }
    "return sorted batches from query sort" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val query = new Query(sft.getTypeName, filter)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            query.setSortBy(Array(org.locationtech.geomesa.filter.ff.sort("dtg", SortOrder.ASCENDING)))
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features, ordered = true)
            }
          }
        }
      }
      ok
    }
    "return sampled arrow encoded data" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          val query = new Query(sft.getTypeName, Filter.INCLUDE)
          query.getHints.put(QueryHints.ARROW_ENCODE, true)
          query.getHints.put(QueryHints.SAMPLING, 0.2f)
          query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          val out = new ByteArrayOutputStream
          results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
          def in() = new ByteArrayInputStream(out.toByteArray)
          WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
            // we don't know exactly which features will be selected
            val expected = SelfClosingIterator(reader.features()).flatMap(f => features.find(_.getID == f.getID)).toSeq
            compare(reader.features(), expected)
            expected.length must beLessThan(10)
          }
        }
      }
      ok
    }
    "return sorted, dictionary encoded projections for non-indexed attributes and nulls" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val transform = Array("team", "weight", "dtg", "geom")
            val query = new Query(sft.getTypeName, filter, transform)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "team,weight")
            query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features, transform.toSeq)
              reader.dictionaries.keySet mustEqual Set("team", "weight")
              reader.dictionaries.apply("weight").iterator.toSeq must contain(null: AnyRef)
            }
          }
        }
      }
      ok
    }
    "return sorted, dictionary encoded projections list type attributes" in {
      dataStores.foreach { ds =>
        filters.dropRight(1).foreach { filter =>
          val transform = Array("names", "dtg", "geom")
          val query = new Query(listSft.getTypeName, filter, transform)
          query.getHints.put(QueryHints.ARROW_ENCODE, true)
          query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "names")
          query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
          query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          val out = new ByteArrayOutputStream
          results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
          def in() = new ByteArrayInputStream(out.toByteArray)
          WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
            compare(reader.features(), listFeatures, transform.toSeq)
            reader.dictionaries.keySet mustEqual Set("names")
            reader.dictionaries.apply("names").iterator.toSeq must
                containTheSameElementsAs(Seq.tabulate(2)(i => s"name0$i"))
          }
        }
      }
      ok
    }
    "return sorted, dictionary encoded projections for different attribute queries" in {
      val filter = ECQL.toFilter("name IN('name0', 'name1')")
      val transforms = Seq(
        Array("dtg", "geom"),
        Array("name", "dtg", "geom"),
        Array("team", "dtg", "geom"),
        Array("name", "team", "dtg", "geom"))
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          foreach(transforms) { transform =>
            val query = new Query(sft.getTypeName, filter, transform)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            val dictionaries = Option(transform.toSeq.filter(t => t != "dtg" && t != "geom")).filter(_.nonEmpty)
            dictionaries.foreach(d => query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, d.mkString(",")))
            query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features, transform.toSeq)
              reader.dictionaries.keySet mustEqual dictionaries.map(_.toSet).getOrElse(Set.empty)
            }
          }
        }
      }
      ok
    }
    "sort on dictionary encoded attributes" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          filters.foreach { filter =>
            val transform = Array("team", "weight", "dtg", "geom")
            val query = new Query(sft.getTypeName, filter, transform)
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "team,weight,dtg")
            query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features, transform.toSeq)
              reader.dictionaries.keySet mustEqual Set("team", "weight", "dtg")
              reader.dictionaries.apply("weight").iterator.toSeq must contain(null: AnyRef)
            }
          }
        }
      }
      ok
    }
    "work with different batch sizes" in {
      dataStores.foreach { ds =>
        sfts.foreach { case (sft, features) =>
          foreach(Seq(2, 4, 10, 20)) { batchSize =>
            val query = new Query(sft.getTypeName, Filter.INCLUDE)
            // ensure we create a unique cache key so we can test out the batch size change
            query.getHints.put(QueryHints.ARROW_ENCODE, true)
            query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
            query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
            query.getHints.put(QueryHints.ARROW_DICTIONARY_VALUES, "name,name0,name1,name2,foo,bar,baz")
            query.getHints.put(QueryHints.ARROW_BATCH_SIZE, batchSize)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
            val out = new ByteArrayOutputStream
            results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
            def in() = new ByteArrayInputStream(out.toByteArray)
            WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
              compare(reader.features(), features)
            }
          }
        }
      }
      ok
    }
    "handle errors in the underlying scan" in {
      sfts.foreach { case (sft, features) =>
        val source = mock[SortedKeyValueIterator[Key, Value]]
        val idx = ds.manager.indices(sft).find(_.name.contains("z3")).getOrElse {
          throw new RuntimeException("Couldn't find index")
        }
        val options = ArrowIterator.configure(sft, idx, ds.stats, None, None, new Hints())._1.getOptions

        val writable = WritableFeature.wrapper(sft, ds.adapter.groups).wrap(features.head)
        val kv = idx.createConverter().convert(writable) match {
          case kv: SingleRowKeyValue[_] => kv
          case _ => throw new RuntimeException("got multiple kvs")
        }

        // mocks
        source.hasTop returns true
        source.getTopKey returns new Key(kv.row)
        source.getTopValue returns new Value(kv.values.head.value)
        source.next() throws new NullPointerException("testing NPE")

        val iter = new ArrowIterator()
        iter.init(source, options, null)
        iter.seek(new org.apache.accumulo.core.data.Range(), java.util.Collections.emptyList(), inclusive = true)
        iter.hasTop must beTrue // we had a valid row before seeing an error, so it gets returned
        iter.next()
        iter.hasTop must beFalse // we see the error again, but this time there were no valid rows first
      }
      ok
    }
  }

  step {
    allocator.close()
    localDs.dispose()
  }
}
