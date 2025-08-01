/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.io.AvroDataFileWriter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class AvroConverterTest extends Specification with AvroUtils with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))

  "AvroConverter should" should {

    "properly convert a GenericRecord to a SimpleFeature" >> {
      val confWithReferencedSchema = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema-file = "/schema.avsc"
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

      val avroSchema =
        WithClose(getClass.getClassLoader.getResourceAsStream("schema.avsc"))(IOUtils.toString(_, StandardCharsets.UTF_8))
      val confWithInlineSchema =
        confWithReferencedSchema.withoutPath("schema-file").withValue("schema", ConfigValueFactory.fromAnyRef(avroSchema))

      foreach(Seq(confWithReferencedSchema, confWithInlineSchema)) { conf =>
        WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
          val ec = converter.createEvaluationContext()
          val res = WithClose(converter.process(new ByteArrayInputStream(bytes), ec))(_.toList)
          res must haveLength(1)
          val sf = res.head
          sf.getAttributeCount must be equalTo 2
          sf.getAttribute("dtg") must not(beNull)

          ec.failure.getCount mustEqual 0L
          ec.success.getCount mustEqual 1L
          ec.stats.failure(0) mustEqual 0L
          ec.stats.success(0) mustEqual 1L
          ec.line mustEqual 1L  // only 1 record passed in itr
        }
      }
    }

    "convert user data" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema-file = "/schema.avsc"
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

        ec.failure.getCount mustEqual 0L
        ec.success.getCount mustEqual 1L
        ec.stats.failure(0) mustEqual 0L
        ec.stats.success(0) mustEqual 1L
        ec.line mustEqual 1L  // only 1 record passed in itr
      }
    }

    "make avro bytes available as $0 with defined schemas" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema-file = "/schema.avsc"
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
          sf.getID mustEqual "3fd4a849601fa2d97dca58043deb9ead" // Hashing.md5().hashBytes(bytes).toString
          sf.getAttributeCount must be equalTo 2
          sf.getAttribute("dtg") must not(beNull)
        }

        ec.failure.getCount mustEqual 0L
        ec.success.getCount mustEqual 2L
        ec.stats.failure(0) mustEqual 0L
        ec.stats.success(0) mustEqual 2L
        ec.line mustEqual 2L
      }
    }

    "make avro bytes available as $0 with embedded schemas" >> {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
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
          sf.getID mustEqual "3fd4a849601fa2d97dca58043deb9ead" // Hashing.md5().hashBytes(bytes).toString
          sf.getAttributeCount must be equalTo 2
          sf.getAttribute("dtg") must not(beNull)
        }

        ec.failure.getCount mustEqual 0L
        ec.success.getCount mustEqual 2L
        ec.stats.failure(0) mustEqual 0L
        ec.stats.success(0) mustEqual 2L
        ec.line mustEqual 2L
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

      val inferred = new AvroConverterFactory().infer(new ByteArrayInputStream(bytes), None, Map.empty[String, AnyRef])

      inferred must beASuccessfulTry
      inferred.get._1 mustEqual sft

      logger.trace(inferred.get._2.root().render(ConfigRenderOptions.concise().setFormatted(true)))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val converted = SelfClosingIterator(converter.process(new ByteArrayInputStream(bytes))).toList

        converted must containTheSameElementsAs(features)
        converted.map(_.getUserData.get("foo")) must containTheSameElementsAs(Seq.tabulate(10)(i => s"bar$i"))
      }
    }

    "automatically convert geomesa avro files with lenient matching" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val features = Seq.tabulate(10) { i =>
        ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, s"2018-01-01T0$i:00:00.000Z", s"POINT(4$i 55)")
      }

      val out = new ByteArrayOutputStream()
      WithClose(new AvroDataFileWriter(out, sft))(writer => features.foreach(writer.append))

      val bytes = out.toByteArray

      val updated = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326,tag:String")
      val inferred = new AvroConverterFactory().infer(new ByteArrayInputStream(bytes), Some(updated), Map.empty[String, AnyRef])

      inferred must beASuccessfulTry
      inferred.get._1 mustEqual sft

      logger.trace(inferred.get._2.root().render(ConfigRenderOptions.concise().setFormatted(true)))

      WithClose(SimpleFeatureConverter(updated, inferred.get._2)) { converter =>
        converter must not(beNull)

        val converted = SelfClosingIterator(converter.process(new ByteArrayInputStream(bytes))).toList

        converted must containTheSameElementsAs(features.map(ScalaSimpleFeature.retype(updated, _)))
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
          |    { "name": "list", "type": { "type": "array", "items": "string" }},
               { "name": "map", "type": { "type": "map", "values": "int" }},
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
          val list = new GenericData.Array[String](schema.getField("list").schema(), Seq(s"$i", s"${i+1}").asJava)
          rec.put("list", list)
          rec.put("map", Map[String, Int]("one" -> i, "two" -> {i + 1}).asJava)
          val props = new GenericData.Record(schema.getField("props").schema())
          props.put("age", i)
          props.put("weight", 10f + i)
          rec.put("props", props)
          writer.append(rec)
          i += 1
        }
      }

      val bytes = out.toByteArray

      val inferred = new AvroConverterFactory().infer(new ByteArrayInputStream(bytes), None, Map.empty[String, AnyRef])

      inferred must beASuccessfulTry

      val expectedSft = SimpleFeatureTypes.createType(inferred.get._1.getTypeName,
        "lat:Double,lon:Double,label:String,list:List[String],map:Map[String,Int],age:Int,weight:Float,*geom:Point:srid=4326")
      inferred.get._1 mustEqual expectedSft

      logger.trace(inferred.get._2.root().render(ConfigRenderOptions.concise().setFormatted(true)))

      WithClose(SimpleFeatureConverter(inferred.get._1, inferred.get._2)) { converter =>
        converter must not(beNull)

        val converted = SelfClosingIterator(converter.process(new ByteArrayInputStream(bytes))).toList
        converted must not(beEmpty)

        val expected = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(expectedSft, s"$i",
            40d + i, 50d + i, s"name$i", Seq(s"$i", s"${i+1}").asJava,
            Map[String, Int]("one" -> i, "two" -> {i + 1}).asJava, i, 10f + i, s"POINT (${ 50d + i } ${ 40d + i })")
        }

        // note: feature ids won't be the same
        converted.map(_.getAttributes) must containTheSameElementsAs(expected.map(_.getAttributes))
      }
    }

    "calculate record bytes for union-type schemas" >> {
      val schema = parser.parse(getClass.getClassLoader.getResourceAsStream("union.avsc"))

      // generate the test data - this is stored in resources already
      if (false) {
        val person = new GenericData.Record(schema.getTypes.get(0))
        Seq("name" -> "pname", "age" -> 21, "location" -> "POINT (45 55)").foreach { case (k, v) => person.put(k, v) }
        val animal = new GenericData.Record(schema.getTypes.get(1))
        Seq("name" -> "aname", "breed" -> "pug", "location" -> "POINT (1 2)").foreach { case (k, v) => animal.put(k, v) }
        val file = new File("union.avro")
        val datumWriter = new GenericDatumWriter[GenericRecord](schema)
        val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
        dataFileWriter.create(schema, file)
        dataFileWriter.append(person)
        dataFileWriter.append(animal)
        dataFileWriter.close()
      }

      val sft = SimpleFeatureTypes.createType("union", "name:String,*geom:Point:srid=4326")

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type        = "avro"
          |   schema      = "embedded"
          |   id-field    = "md5($0)"
          |   fields = [
          |     { name = "name", transform = "avroPath($1, '/name')" },
          |     { name = "geom", transform = "point(avroPath($1, '/location'))" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()

        // pass two messages to check message buffering for record bytes
        val res = WithClose(converter.process(getClass.getClassLoader.getResourceAsStream("union.avro"), ec))(_.toList)
        res must haveLength(2)
        res(0).getAttribute(0) mustEqual "pname"
        res(1).getAttribute(0) mustEqual "aname"
        res(0).getAttribute(1).toString mustEqual "POINT (45 55)"
        res(1).getAttribute(1).toString mustEqual "POINT (1 2)"
      }
    }
  }
}
