/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, HBaseCatalogParam}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.CountStat
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseSamplingFilterTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  "Hbase" should {
    "working with sampling" in {
      val typeName = "testSampling"

      val params = Map(
        ConnectionParam.getName -> MiniCluster.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName
      )
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName,
          "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled=s2:geom"))
        val sft = ds.getSchema(typeName)

        val features =
          (0 until 10).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track1", s"2010-05-07T0$i:00:00.000Z", s"POINT(40 6$i)")
          } ++ (10 until 20).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track2", s"2010-05-${i}T$i:00:00.000Z", s"POINT(40 6${i - 10})")
          } ++ (20 until 30).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track3", s"2010-05-${i}T${i-10}:00:00.000Z", s"POINT(40 8${i - 20})")
          }

        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(f => FeatureUtils.write(writer, f, useProvidedFid = true))
        }

        def runQuery(query: Query): Seq[SimpleFeature] =
          SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

        {
          // 0

          //sampling disabled

          //this filter return all feature i need to test sampling return size 30 without sampling
          val filter = "bbox(geom, -179, -89, 179, 89)"+
            " AND dtg between '2009-05-07T00:00:00.000Z' and '2011-05-08T00:00:00.000Z'"

          val query = new Query(sft.getTypeName, ECQL.toFilter(filter), Array("name","track"))

          val features = runQuery(query)

          features must haveSize(30)
        }

        {
          // 1
          //filter enabled
          //trasformer disabled
          //sample-by enabled

          //this filter return all feature i need to test sampling return size 30 without sampling
          val filter = "bbox(geom, -179, -89, 179, 89)"+
            " AND dtg between '2009-05-07T00:00:00.000Z' and '2011-05-08T00:00:00.000Z'"

          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(QueryHints.SAMPLING, 0.1f)
          query.getHints.put(QueryHints.SAMPLE_BY, "track")

          val features = runQuery(query)

          features must haveSize(12)

          features(0).getAttribute("dtg") must not beNull

          features.filter(p=>p.getAttribute("track").equals("track1")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track2")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track3")).size must greaterThan(1)

        }


        {
          // 2
          //filter enabled
          //trasformer enabled
          //sample-by enabled

          //this filter return all feature i need to test sampling
          val filter = "bbox(geom, -179, -89, 179, 89)"+
            " AND dtg between '2009-05-07T00:00:00.000Z' and '2011-05-08T00:00:00.000Z'"

          val query = new Query(sft.getTypeName, ECQL.toFilter(filter), Array("name","track"))
          query.getHints.put(QueryHints.SAMPLING, 0.1f)
          query.getHints.put(QueryHints.SAMPLE_BY, "track")

          val features = runQuery(query)

          features must haveSize(12)

          features(0).getAttribute("dtg") must beNull

          features.filter(p=>p.getAttribute("track").equals("track1")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track2")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track3")).size must greaterThan(1)


        }

        {
          // 3
          //filter disabled
          //trasformer enabled
          //sample-by enabled

          val query = new Query(sft.getTypeName)
          query.setPropertyNames(Array("name","track"))
          query.getHints.put(QueryHints.SAMPLING, 0.1f)
          query.getHints.put(QueryHints.SAMPLE_BY, "track")

          val features = runQuery(query)

          features must haveSize(12)

          features(0).getAttribute("dtg") must beNull

          features.filter(p=>p.getAttribute("track").equals("track1")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track2")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track3")).size must greaterThan(1)

        }

        {
          // 4
          //filter disabled
          //trasformer disabled
          //sample-by enabled


          val query = new Query(sft.getTypeName)
          query.getHints.put(QueryHints.SAMPLING, 0.1f)
          query.getHints.put(QueryHints.SAMPLE_BY, "track")

          val features = runQuery(query)

          features must haveSize(12)

          features(0).getAttribute("dtg") must not beNull

          features.filter(p=>p.getAttribute("track").equals("track1")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track2")).size must greaterThan(1)
          features.filter(p=>p.getAttribute("track").equals("track3")).size must greaterThan(1)

        }

        {
          // 5
          //filter enabled
          //trasformer disabled
          //sample-by disabled

          //this filter return all feature i need to test sampling
          val filter = "bbox(geom, -179, -89, 179, 89)"+
            " AND dtg between '2009-05-07T00:00:00.000Z' and '2011-05-08T00:00:00.000Z'"

          val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
          query.getHints.put(QueryHints.SAMPLING, 0.1f)

          val features = runQuery(query)

          features must haveSize(4)

          features(0).getAttribute("dtg") must not beNull

        }

        {
          // 6
          //filter enabled
          //trasformer enabled
          //sample-by disabled


          //this filter return all feature i need to test sampling
          val filter = "bbox(geom, -179, -89, 179, 89)"+
            " AND dtg between '2009-05-07T00:00:00.000Z' and '2011-05-08T00:00:00.000Z'"

          val query = new Query(sft.getTypeName, ECQL.toFilter(filter), Array("name","track"))
          query.getHints.put(QueryHints.SAMPLING, 0.1f)

          val features = runQuery(query)

          features must haveSize(4)

          features(0).getAttribute("dtg") must beNull
        }
        {
          // 7
          //filter disabled
          //trasformer enabled
          //sample-by disabled

          val query = new Query(sft.getTypeName)
          query.setPropertyNames(Array("name","track"))
          query.getHints.put(QueryHints.SAMPLING, 0.1f)

          val features = runQuery(query)

          features must haveSize(4)

          features(0).getAttribute("dtg") must beNull

        }
        {
          // 8
          //filter disabled
          //trasformer disabled
          //sample-by disabled

          val query = new Query(sft.getTypeName)
          query.getHints.put(QueryHints.SAMPLING, 0.1f)

          val features = runQuery(query)

          features must haveSize(4)
          features(0).getAttribute("dtg") must not beNull
        }

        {
          //check interaction with aggregations

          val query = new Query(sft.getTypeName)
          query.getHints.put(QueryHints.STATS_STRING, "Count()")
          query.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)
          query.getHints.put(QueryHints.SAMPLING, 0.1f)

          val features = runQuery(query)

          val stat:CountStat = StatsScan.decodeStat(sft)(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
                                  .next.getAttribute(0).asInstanceOf[String]).asInstanceOf[CountStat]

          stat.count must beEqualTo(4)
        }


      } finally {
        ds.dispose()
      }
    }
  }
}
