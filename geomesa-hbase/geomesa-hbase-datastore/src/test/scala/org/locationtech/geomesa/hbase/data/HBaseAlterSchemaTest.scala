/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.geotools.data.{Query, Transaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, HBaseCatalogParam}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseAlterSchemaTest extends Specification {

  "HBaseDataStore" should {
    "update schemas" in {
      val params = Map(
        ConnectionParam.getName -> MiniCluster.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName
      )
      WithStore[HBaseDataStore](params) { ds =>
        var sft = SimpleFeatureTypes.createType("alterschema", "name:String:index=true,age:Int,dtg:Date,*geom:Point:srid=4326")
        ds.createSchema(sft)
        sft = ds.getSchema("alterschema")
        val feature = ScalaSimpleFeature.create(sft, "0", "name0", 0, "2018-01-01T06:00:00.000Z", "POINT (40 55)")
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, feature, useProvidedFid = true)
        }

        var filters = Seq(
          "name = 'name0'",
          "bbox(geom,38,53,42,57)",
          "bbox(geom,38,53,42,57) AND dtg during 2018-01-01T00:00:00.000Z/2018-01-01T12:00:00.000Z",
          "IN ('0')"
        ).map(ECQL.toFilter)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(feature)
        }
        ds.stats.getCount(sft, exact = true) must beSome(1L)

        // rename
        ds.updateSchema(sft.getTypeName, SimpleFeatureTypes.renameSft(sft, "rename"))

        sft = ds.getSchema("rename")
        sft must not(beNull)
        sft.getTypeName mustEqual "rename"
        ds.getSchema("alterschema") must beNull

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft, exact = true) must beSome(1L)
        ds.stats.getMinMax[String](sft, "name", exact = true).map(_.max) must beSome("name0")

        // rename column
        Some(new SimpleFeatureTypeBuilder()).foreach{ builder =>
          builder.init(sft)
          builder.set(0, SimpleFeatureSpecParser.parseAttribute("names:String:index=true").toDescriptor)
          val update = builder.buildFeatureType()
          update.getUserData.putAll(sft.getUserData)
          ds.updateSchema(sft.getTypeName, update)
        }

        sft = ds.getSchema(sft.getTypeName)
        sft must not(beNull)
        sft.getTypeName mustEqual "rename"
        sft.getDescriptor(0).getLocalName mustEqual "names"
        sft.getDescriptor("names") mustEqual sft.getDescriptor(0)

        filters = Seq(ECQL.toFilter("names = 'name0'")) ++ filters.drop(1)

        forall(filters) { filter =>
          val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)
          SelfClosingIterator(reader).toList mustEqual Seq(ScalaSimpleFeature.copy(sft, feature))
        }
        ds.stats.getCount(sft, exact = true) must beSome(1L)
        ds.stats.getMinMax[String](sft, "names", exact = true).map(_.max) must beSome("name0")
      }
    }
  }
}
