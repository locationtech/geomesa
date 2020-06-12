/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.vector.ipc.message.IpcOption
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DictionaryBuildingWriterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }

  val ipcOpts = new IpcOption() // TODO test legacy opts

  "SimpleFeatureVector" should {
    "dynamically encode dictionary values" >> {
      val out = new ByteArrayOutputStream()
      WithClose(new DictionaryBuildingWriter(sft, Seq("name"), SimpleFeatureEncoding.Max, ipcOpts)) { writer =>
        features.foreach(writer.add)
        writer.encode(out)
      }
      WithClose(SimpleFeatureArrowFileReader.streaming(() => new ByteArrayInputStream(out.toByteArray))) { reader =>
        reader.dictionaries must haveSize(1)
        reader.dictionaries.get("name") must beSome
        reader.dictionaries("name").iterator.toSeq must containTheSameElementsAs(Seq("name00", "name01"))

        WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features)
      }
    }
  }
}
