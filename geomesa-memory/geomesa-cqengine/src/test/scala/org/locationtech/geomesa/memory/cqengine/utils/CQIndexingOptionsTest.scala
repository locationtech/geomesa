/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.util.Date

import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.index.navigable.NavigableIndex
import com.googlecode.cqengine.index.radix.RadixTreeIndex
import com.googlecode.cqengine.index.support.AbstractAttributeIndex
import com.googlecode.cqengine.index.unique.UniqueIndex
import com.vividsolutions.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexingOptions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CQIndexingOptionsTest extends Specification {

  val spec = "Who:String:cq-index=default," +
             "What:Integer:cq-index=unique," +
             "When:Date:cq-index=navigable," +
             "*Where:Point:srid=4326," +
             "Why:String:cq-index=hash," +
             "BadIndex:String:cq-index=foo"

  "CQ Indexing options" should {
    "be configurable from SimpleFeatureTypes" >> {
      "via SFT spec" >> {
        val sft = SimpleFeatureTypes.createType("test", spec)

        val whoDescriptor = sft.getDescriptor("Who")
        getCQIndexType(whoDescriptor) mustEqual CQIndexType.DEFAULT

        val whatDescriptor = sft.getDescriptor("What")
        getCQIndexType(whatDescriptor) mustEqual CQIndexType.UNIQUE

        val whenDescriptor = sft.getDescriptor("When")
        getCQIndexType(whenDescriptor) mustEqual CQIndexType.NAVIGABLE

        val whereDescriptor = sft.getDescriptor("Where")
        getCQIndexType(whereDescriptor) mustEqual CQIndexType.NONE

        val whyDescriptor = sft.getDescriptor("Why")
        getCQIndexType(whyDescriptor) mustEqual CQIndexType.HASH

        val badIndexDescriptor = sft.getDescriptor("BadIndex")
        getCQIndexType(badIndexDescriptor) mustEqual CQIndexType.NONE
      }

      "via setCQIndexType" >> {
        val sft = SimpleFeatureTypes.createType("test", spec)

        val originalWhoDescriptor = sft.getDescriptor("Who")
        getCQIndexType(originalWhoDescriptor) mustEqual CQIndexType.DEFAULT

        setCQIndexType(originalWhoDescriptor, CQIndexType.HASH)
        getCQIndexType(originalWhoDescriptor) mustEqual CQIndexType.HASH
      }
    }
    "build IndexedCollections with indices" >> {
      val sft = SimpleFeatureTypes.createType("test", spec)

      val cq = new GeoCQEngine(sft)
      val indexes: List[AbstractAttributeIndex[_, SimpleFeature]] =
        cq.cqcache.getIndexes.toList.collect{ case a: AbstractAttributeIndex[_, SimpleFeature] => a }

      val nameToIndex: Map[String, AbstractAttributeIndex[_, SimpleFeature]] = indexes.map {
        index => index.getAttribute.getAttributeName -> index
      }.toMap

      // NB: Where is the default geometry field, so it should be indexed with a GeoIndex
      nameToIndex("Where") must beAnInstanceOf[GeoIndex[Geometry, SimpleFeature]]

      // Who is a string field and the 'default' hint is used.  This should be a Radix index
      nameToIndex("Who")   must beAnInstanceOf[RadixTreeIndex[String, SimpleFeature]]
      nameToIndex("What")  must beAnInstanceOf[UniqueIndex[Integer, SimpleFeature]]
      nameToIndex("When")  must beAnInstanceOf[NavigableIndex[Date, SimpleFeature]]
      nameToIndex("Why")   must beAnInstanceOf[HashIndex[String, SimpleFeature]]
    }

  }

}
