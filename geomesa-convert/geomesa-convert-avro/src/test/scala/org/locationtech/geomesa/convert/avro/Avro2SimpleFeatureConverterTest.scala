package org.locationtech.geomesa.convert.avro

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Avro2SimpleFeatureConverterTest extends Specification with AvroUtils {

  "Avro2SimpleFeature should" should {

    val conf = ConfigFactory.parseString(
      """
        | converter = {
        |   type   = "avro"
        |   schema = "/schema.avsc"
        |   sft    = "testsft"
        |   id-field = "uuid()"
        |   fields = [
        |     { name = "lat",  transform = "avroPath($1, '/content$type=TObj/kvmap[$k=lat]/v')" },
        |     { name = "lon",  transform = "avroPath($1, '/content$type=TObj/kvmap[$k=lon]/v')" },
        |     { name = "geom", transform = "point($lon, $lat)" }
        |   ]
        | }
      """.stripMargin)

    "properly convert a GenericRecord to a SimpleFeature" >> {
      val converter = SimpleFeatureConverters.build[Array[Byte]](conf)
      val sf = converter.processSingleInput(bytes)
      sf.getAttributeCount must be equalTo 1
    }
  }
}
