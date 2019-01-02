/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.util.Date

import com.googlecode.cqengine.index.AttributeIndex
import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.index.navigable.NavigableIndex
import com.googlecode.cqengine.index.radix.RadixTreeIndex
import com.googlecode.cqengine.index.unique.UniqueIndex
import org.locationtech.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
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
             "*Where:Point:srid=4326:cq-index=geometry," +
             "Why:String:cq-index=hash"

  "CQ Indexing options" should {
    "be configurable via SFT spec" >> {
      val sft = SimpleFeatureTypes.createType("test", spec)
      val types = CQIndexType.getDefinedAttributes(sft)

      types must contain(("Who", CQIndexType.DEFAULT))
      types must contain(("What", CQIndexType.UNIQUE))
      types must contain(("When", CQIndexType.NAVIGABLE))
      types must contain(("Where", CQIndexType.GEOMETRY))
      types must contain(("Why", CQIndexType.HASH))
    }

    "be configurable via setCQIndexType" >> {
      val sft = SimpleFeatureTypes.createType("test", spec)
      CQIndexType.getDefinedAttributes(sft) must contain(("Who", CQIndexType.DEFAULT))
      sft.getDescriptor("Who").getUserData.put("cq-index", CQIndexType.HASH.toString)
      CQIndexType.getDefinedAttributes(sft) must contain(("Who", CQIndexType.HASH))
    }

    "fail for invalid index types" in {
      val sft = SimpleFeatureTypes.createType("test", spec + ",BadIndex:String:cq-index=foo")
      CQIndexType.getDefinedAttributes(sft) must throwAn[Exception]
    }

    "build IndexedCollections with indices" >> {
      val sft = SimpleFeatureTypes.createType("test", spec)

      val nameToIndex = scala.collection.mutable.Map.empty[String, AttributeIndex[_, SimpleFeature]]

      new GeoCQEngine(sft, CQIndexType.getDefinedAttributes(sft)) {
        cqcache.getIndexes.foreach {
          case a: AttributeIndex[_, SimpleFeature] => nameToIndex.put(a.getAttribute.getAttributeName, a)
          case _ => // no-op
        }
      }

      nameToIndex.get("Where") must beSome[AttributeIndex[_, SimpleFeature]](beAnInstanceOf[GeoIndex[Geometry, SimpleFeature]])

      // Who is a string field and the 'default' hint is used.  This should be a Radix index
      nameToIndex.get("Who")  must beSome[AttributeIndex[_, SimpleFeature]](beAnInstanceOf[RadixTreeIndex[String, SimpleFeature]])
      nameToIndex.get("What") must beSome[AttributeIndex[_, SimpleFeature]](beAnInstanceOf[UniqueIndex[Integer, SimpleFeature]])
      nameToIndex.get("When") must beSome[AttributeIndex[_, SimpleFeature]](beAnInstanceOf[NavigableIndex[Date, SimpleFeature]])
      nameToIndex.get("Why")  must beSome[AttributeIndex[_, SimpleFeature]](beAnInstanceOf[HashIndex[String, SimpleFeature]])
    }

  }

}
