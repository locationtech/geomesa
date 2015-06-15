/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding.serialization

import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.config.Config
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.{ScalaSimpleFeature, ScalaSimpleFeatureFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Kryo2SimpleFeatureSerializerTest extends Specification {

  "Kryo2SimpleFeatureSerializer" should {
    "read and write simple features" in {
      val kryo = new SimpleFeatureKryoHadoop(new Config(){
        override def set(key: String, value: String) = {}
        override def get(key: String) = null
      }).newKryo()

      val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val sf = ScalaSimpleFeatureFactory.buildFeature(sft, Seq("myname", "2014-01-10T00:00:00.000Z", "POINT(45 46)"), "fid-1")

      val output = new Output(1024, -1)
      val input = new Input(Array.empty[Byte])

      kryo.writeObject(output, sf)
      input.setBuffer(output.toBytes)
      val deserialized = kryo.readObject(input, classOf[ScalaSimpleFeature])

      deserialized mustEqual(sf)
      deserialized.getFeatureType mustEqual(sft)
    }
  }
}
