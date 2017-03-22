/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.iterators.{KryoAttributeKeyValueIterator, KryoLazyFilterTransformIterator}
import org.locationtech.geomesa.accumulo.iterators.KryoLazyFilterTransformIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AttrKeyPlusValueIteratorTest extends Specification with TestWithMultipleSfts {

  val spec =
    "name:String:index=true:cardinality=high," +
    "age:Integer:index=false," +
    "count:Long:index=false," +
    "dtg:Date:default=true," +
    "*geom:Point:srid=4326"

  val dtf = ISODateTimeFormat.dateTime().withZoneUTC()

  def features(sft: SimpleFeatureType) = Seq(
    Array("alice",   20,   1, dtf.parseDateTime("2014-01-01T12:00:00.000Z").toDate, WKTUtils.read("POINT(45.0 49.0)")),
    Array("bill",    21,   2, dtf.parseDateTime("2014-01-02T12:00:00.000Z").toDate, WKTUtils.read("POINT(46.0 49.0)")),
    Array("bob",     30,   3, dtf.parseDateTime("2014-01-03T12:00:00.000Z").toDate, WKTUtils.read("POINT(47.0 49.0)")),
    Array("charles", null, 4, dtf.parseDateTime("2014-01-04T12:00:00.000Z").toDate, WKTUtils.read("POINT(48.0 49.0)"))
  ).map { entry =>
    val feature = new ScalaSimpleFeature(entry.head.toString, sft)
    feature.setAttributes(entry.asInstanceOf[Array[AnyRef]])
    feature
  }

  // TODO GEOMESA-1146 refactor to allow running of tests with table sharing on and off...

  "Query planning" should {

    "work with table sharing on" >> {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      val sft = createNewSchema(spec)
      addFeatures(sft, features(sft))
      val sftName = sft.getTypeName
      val fs = ds.getFeatureSource(sftName)

      sft.isTableSharing must beTrue

      "do a single scan for attribute idx queries" >> {
        val filterName = "(((name = 'alice') or name = 'bill') or name = 'bob')"
        val filter = ECQL.toFilter(s"$filterName AND BBOX(geom, 40, 40, 60, 60) and " +
            s"dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z ")

        val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
        val plans = ds.getQueryPlan(query)
        plans must haveLength(1)
        plans.head must beAnInstanceOf[BatchScanPlan]
        val bsp = plans.head.asInstanceOf[BatchScanPlan]
        bsp.iterators must haveLength(2)
        bsp.iterators.map(_.getIteratorClass) must containTheSameElementsAs {
          Seq(classOf[KryoLazyFilterTransformIterator].getName, classOf[KryoAttributeKeyValueIterator].getName)
        }

        val rws = fs.getFeatures(query).features().toList
        rws must haveLength(3)
        val alice = rws.filter(_.get[String]("name") == "alice").head
        alice.getID mustEqual "alice"
        alice.getAttributeCount mustEqual 3
      }

      "work with 150 attrs" >> {
        import scala.collection.JavaConversions._
        val ff = CommonFactoryFinder.getFilterFactory2
        val filterName = ff.or((0 to 150).map(i => ff.equals(ff.property("name"), ff.literal(i.toString))))
        val filter = ff.and(filterName, ECQL.toFilter("BBOX(geom, 40, 40, 60, 60) and " +
            "dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z "))

        val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
        val plans = ds.getQueryPlan(query)
        plans.size mustEqual 1
        plans.head must beAnInstanceOf[BatchScanPlan]
        val bsp = plans.head.asInstanceOf[BatchScanPlan]
        bsp.iterators must haveLength(2)
        bsp.iterators.map(_.getIteratorClass) must containTheSameElementsAs {
          Seq(classOf[KryoLazyFilterTransformIterator].getName, classOf[KryoAttributeKeyValueIterator].getName)
        }
      }

      "support sampling" >> {
        // note: sampling is per-iterator, so an 'OR =' query usually won't reduce the results
        val filterName = "name > 'alice'"
        val filter = ECQL.toFilter(s"$filterName AND BBOX(geom, 40, 40, 60, 60) and " +
            s"dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z ")

        val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
        query.getHints.put(QueryHints.SAMPLING, new java.lang.Float(.5f))

        val plans = ds.getQueryPlan(query)
        plans.size mustEqual 1
        plans.head must beAnInstanceOf[BatchScanPlan]
        val bsp = plans.head.asInstanceOf[BatchScanPlan]
        bsp.iterators must haveLength(2)
        bsp.iterators.map(_.getIteratorClass) must containTheSameElementsAs {
          Seq(classOf[KryoLazyFilterTransformIterator].getName, classOf[KryoAttributeKeyValueIterator].getName)
        }

        val rws = fs.getFeatures(query).features().toList
        rws must haveLength(2)
        rws.head.getAttributeCount mustEqual 3
      }
    }

    "work with table sharing off" >> {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val sft = createNewSchema(spec, tableSharing = false)
      addFeatures(sft, features(sft))
      val sftName = sft.getTypeName
      val fs = ds.getFeatureSource(sftName)

      sft.isTableSharing must beFalse

      "do a single scan for attribute idx queries" >> {
        val filterName = "(((name = 'alice') or name = 'bill') or name = 'bob')"
        val filter = ECQL.toFilter(s"$filterName AND BBOX(geom, 40, 40, 60, 60) and " +
            s"dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z ")
        val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
        val plans = ds.getQueryPlan(query)
        plans.size mustEqual 1
        plans.head must beAnInstanceOf[BatchScanPlan]
        val bsp = plans.head.asInstanceOf[BatchScanPlan]
        bsp.iterators must haveLength(2)
        bsp.iterators.map(_.getIteratorClass) must containTheSameElementsAs {
          Seq(classOf[KryoLazyFilterTransformIterator].getName, classOf[KryoAttributeKeyValueIterator].getName)
        }

        val rws = fs.getFeatures(query).features().toList
        rws must haveLength(3)
        val alice = rws.filter(_.get[String]("name") == "alice").head
        alice.getID mustEqual "alice"
        alice.getAttributeCount mustEqual 3
      }

      "work with 150 attrs" >> {
        import scala.collection.JavaConversions._
        val ff = CommonFactoryFinder.getFilterFactory2
        val filterName = ff.or((0 to 150).map(i => ff.equals(ff.property("name"), ff.literal(i.toString))))
        val filter = ff.and(filterName, ECQL.toFilter("BBOX(geom, 40, 40, 60, 60) and " +
            "dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z "))

        val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
        val plans = ds.getQueryPlan(query)
        plans.size mustEqual 1
        plans.head must beAnInstanceOf[BatchScanPlan]
        val bsp = plans.head.asInstanceOf[BatchScanPlan]
        bsp.iterators must haveLength(2)
        bsp.iterators.map(_.getIteratorClass) must containTheSameElementsAs {
          Seq(classOf[KryoLazyFilterTransformIterator].getName, classOf[KryoAttributeKeyValueIterator].getName)
        }
      }

      "support sampling" >> {
        // note: sampling is per-iterator, so an 'OR =' query usually won't reduce the results
        val filterName = "name > 'alice'"
        val filter = ECQL.toFilter(s"$filterName AND BBOX(geom, 40, 40, 60, 60) and " +
            s"dtg during 2014-01-01T00:00:00.000Z/2014-01-05T00:00:00.000Z ")

        val query = new Query(sftName, filter, Array[String]("dtg", "geom", "name"))
        query.getHints.put(QueryHints.SAMPLING, new java.lang.Float(.5f))

        val plans = ds.getQueryPlan(query)
        plans.size mustEqual 1
        plans.head must beAnInstanceOf[BatchScanPlan]
        val bsp = plans.head.asInstanceOf[BatchScanPlan]
        bsp.iterators must haveLength(2)
        bsp.iterators.map(_.getIteratorClass) must containTheSameElementsAs {
          Seq(classOf[KryoLazyFilterTransformIterator].getName, classOf[KryoAttributeKeyValueIterator].getName)
        }

        val rws = fs.getFeatures(query).features().toList
        rws must haveLength(2)
        rws.head.getAttributeCount mustEqual 3
      }
    }
  }
}
