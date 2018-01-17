/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.junit.runner.RunWith
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.utils.text.KVPairParser
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloSftBuilderTest extends Specification {

  sequential

  "SpecBuilder" >> {
    "configure table splitters as strings" >> {
      val config = Map("id.type" -> "digit", "fmt" ->"%02d", "min" -> "0", "max" -> "99")
      val sft1 = new AccumuloSftBuilder()
        .intType("i")
        .longType("l")
        .recordSplitter(classOf[DefaultSplitter].getName, config)
        .build("test")

      // better - uses class directly (or at least less annoying)
      val sft2 = new AccumuloSftBuilder()
        .recordSplitter(classOf[DefaultSplitter], config)
        .intType("i")
        .longType("l")
        .build("test")

      def test(sft: SimpleFeatureType) = {
        import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

        sft.getAttributeCount mustEqual 2
        sft.getAttributeDescriptors.map(_.getLocalName) must containAllOf(List("i", "l"))

        sft.getTableSplitter must beSome(classOf[DefaultSplitter])
        val opts = KVPairParser.parse(sft.getTableSplitterOptions)
        opts.toSeq must containTheSameElementsAs(config.toSeq)
      }

      List(sft1, sft2) forall test
    }
  }
}
