/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.common.hash.Hashing
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroConverterTest extends Specification with AvroUtils with LazyLogging {

  sequential

  val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

  "AvroConverter should" should {

    "properly convert a GenericRecord to a SimpleFeature" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema-file = "/schema.avsc"
          |   sft         = "testsft"
          |   id-field    = "uuid()"
          |   fields = [
          |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
          |     { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
          |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
          |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()
        val res = WithClose(converter.process(new ByteArrayInputStream(bytes), ec))(_.toList)
        res must haveLength(1)
        val sf = res.head
        sf.getAttributeCount must be equalTo 2
        sf.getAttribute("dtg") must not(beNull)

        ec.counter.getFailure mustEqual 0L
        ec.counter.getSuccess mustEqual 1L
        ec.counter.getLineCount mustEqual 1L  // only 1 record passed in itr
      }
    }

    "convert user data" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema-file = "/schema.avsc"
          |   sft         = "testsft"
          |   id-field    = "uuid()"
          |   user-data   = {
          |     my.user.key = "$lat"
          |   }
          |   fields = [
          |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
          |     { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
          |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
          |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()
        val res = WithClose(converter.process(new ByteArrayInputStream(bytes), ec))(_.toList)
        res must haveLength(1)
        val sf = res.head
        sf.getAttributeCount must be equalTo 2
        sf.getAttribute("dtg") must not(beNull)
        sf.getUserData.get("my.user.key") mustEqual 45d

        ec.counter.getFailure mustEqual 0L
        ec.counter.getSuccess mustEqual 1L
        ec.counter.getLineCount mustEqual 1L  // only 1 record passed in itr
      }
    }

    "make avro bytes available as $0 with defined schemas" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema-file = "/schema.avsc"
          |   sft         = "testsft"
          |   id-field    = "md5($0)"
          |   fields = [
          |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
          |     { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
          |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
          |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()

        // pass two messages to check message buffering for record bytes
        val res = WithClose(converter.process(new ByteArrayInputStream(bytes ++ bytes), ec))(_.toList)
        res must haveLength(2)
        foreach(res) { sf =>
          sf.getID mustEqual Hashing.md5().hashBytes(bytes).toString
          sf.getAttributeCount must be equalTo 2
          sf.getAttribute("dtg") must not(beNull)
        }

        ec.counter.getFailure mustEqual 0L
        ec.counter.getSuccess mustEqual 2L
        ec.counter.getLineCount mustEqual 2L
      }
    }

    "make avro bytes available as $0 with embedded schemas" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   sft         = "testsft"
          |   schema      = "embedded"
          |   id-field    = "md5($0)"
          |   fields = [
          |     { name = "tobj", transform = "avroPath($1, '/content$type=TObj')" },
          |     { name = "dtg",  transform = "date('yyyy-MM-dd', avroPath($tobj, '/kvmap[$k=dtg]/v'))" },
          |     { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
          |     { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
          |     { name = "geom", transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val out = new ByteArrayOutputStream()
      WithClose(new DataFileWriter(writer)) { fileWriter =>
        fileWriter.create(schema, out)
        fileWriter.append(obj)
        fileWriter.append(obj)
      }

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()

        // pass two messages to check message buffering for record bytes
        val res = WithClose(converter.process(new ByteArrayInputStream(out.toByteArray), ec))(_.toList)
        res must haveLength(2)
        foreach(res) { sf =>
          sf.getID mustEqual Hashing.md5().hashBytes(bytes).toString
          sf.getAttributeCount must be equalTo 2
          sf.getAttribute("dtg") must not(beNull)
        }

        ec.counter.getFailure mustEqual 0L
        ec.counter.getSuccess mustEqual 2L
        ec.counter.getLineCount mustEqual 2L
      }
    }

    "automatically convert geomesa avro files" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val features = Seq.tabulate(10) { i =>
        val sf = ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, s"2018-01-01T0$i:00:00.000Z", s"POINT(4$i 55)")
        sf.getUserData.put("foo", s"bar$i")
        sf
      }

      val out = new ByteArrayOutputStream()
      WithClose(new AvroDataFileWriter(out, sft))(writer => features.foreach(writer.append))

      val bytes = out.toByteArray

      val inferred = new AvroConverterFactory().infer(new ByteArrayInputStream(bytes))

      inferred must beSome
      inferred.get._1 mustEqual sft

      logger.trace(inferred.get._2.root().render(ConfigRenderOptions.concise().setFormatted(true)))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val converted = SelfClosingIterator(converter.process(new ByteArrayInputStream(bytes))).toList

        converted must containTheSameElementsAs(features)
        converted.map(_.getUserData.get("foo")) must containTheSameElementsAs(Seq.tabulate(10)(i => s"bar$i"))
      }
    }

    "automatically convert arbitrary avro files" >> {
      val schema = parser.parse(
        """{
          |  "name": "MyMessage",
          |  "type": "record",
          |  "fields": [
          |    { "name": "lat", "type": "double" },
          |    { "name": "lon", "type": "double" },
          |    { "name": "label", "type": [ "string", "null" ] },
          |    { "name": "props",
          |      "type": {
          |        "name": "properties",
          |        "type": "record",
          |        "fields": [
          |          { "name": "age", "type": "int" },
          |          { "name": "weight", "type": "float" }
          |        ]
          |      }
          |    }
          |  ]
          |}
        """.stripMargin)

      val out = new ByteArrayOutputStream()

      WithClose(new DataFileWriter(new GenericDatumWriter[GenericRecord]())) { writer =>
        writer.create(schema, out)
        var i = 0
        while (i < 10) {
          val rec = new GenericData.Record(schema)
          rec.put("lat", 40d + i)
          rec.put("lon", 50d + i)
          rec.put("label", s"name$i")
          val props = new GenericData.Record(schema.getField("props").schema())
          props.put("age", i)
          props.put("weight", 10f + i)
          rec.put("props", props)
          writer.append(rec)
          i += 1
        }
      }

      val bytes = out.toByteArray

      val inferred = new AvroConverterFactory().infer(new ByteArrayInputStream(bytes))

      inferred must beSome

      val expectedSft = SimpleFeatureTypes.createType(inferred.get._1.getTypeName,
        "lat:Double,lon:Double,label:String,age:Int,weight:Float,*geom:Point:srid=4326")
      inferred.get._1 mustEqual expectedSft

      logger.trace(inferred.get._2.root().render(ConfigRenderOptions.concise().setFormatted(true)))

      WithClose(SimpleFeatureConverter(inferred.get._1, inferred.get._2)) { converter =>
        converter must not(beNull)

        val converted = SelfClosingIterator(converter.process(new ByteArrayInputStream(bytes))).toList
        converted must not(beEmpty)

        val expected = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(expectedSft, s"$i", 40d + i, 50d + i, s"name$i", i, 10f + i,
            s"POINT (${ 50d + i } ${ 40d + i })")
        }

        // note: feature ids won't be the same
        converted.map(_.getAttributes) must containTheSameElementsAs(expected.map(_.getAttributes))
      }
    }
  }
}
