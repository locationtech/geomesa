/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.stats.Cardinality
import org.locationtech.geomesa.utils.text.KVPairParser
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloSchemaBuilderTest extends Specification {

  import scala.collection.JavaConverters._

  "AccumuloSchemaBuilder" should {

    "allow join indices" >> {
      val spec = AccumuloSchemaBuilder.builder()
          .addString("foo").withJoinIndex()
          .addInt("bar").withJoinIndex(Cardinality.HIGH)
          .spec

      spec mustEqual "foo:String:index=join,bar:Int:index=join:cardinality=high"
    }

    "configure table splitters as strings" >> {
      val config = Map("id.type" -> "digit", "fmt" ->"%02d", "min" -> "0", "max" -> "99")
      val sft1 = AccumuloSchemaBuilder.builder()
          .addInt("i")
          .addLong("l")
          .userData
          .splits(config)
          .build("test")

      // better - uses class directly (or at least less annoying)
      val sft2 = AccumuloSchemaBuilder.builder()
          .userData
          .splits(config)
          .addInt("i")
          .addLong("l")
          .build("test")

      def test(sft: SimpleFeatureType) = {
        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.TABLE_SPLITTER_OPTS

        sft.getAttributeCount mustEqual 2
        sft.getAttributeDescriptors.asScala.map(_.getLocalName) must containAllOf(List("i", "l"))

        val opts = KVPairParser.parse(sft.getUserData.get(TABLE_SPLITTER_OPTS).asInstanceOf[String])
        opts.toSeq must containTheSameElementsAs(config.toSeq)
      }

      List(sft1, sft2) forall test
    }
  }
}
