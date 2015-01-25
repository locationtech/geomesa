package org.locationtech.geomesa.convert.fixedwidth

import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FixedWidthConverterTest extends Specification {

  "FixedWidthConverter" >> {
    val conf = ConfigFactory.parseString(
      """
        | converter = {
        |   type      = "fixed-width"
        |   type-name = "testsft"
        |   id-field  = "uuid()"
        |   fields = [
        |     { name = "lat",  transform = "$0::double", start = 1, width = 2 },
        |     { name = "lon",  transform = "$0::double", start = 3, width = 2 },
        |     { name = "geom", transform = "point($lon, $lat)" }
        |   ]
        | }
      """.stripMargin)

    val converter = SimpleFeatureConverters.build[String](conf)

    "process fixed with data" >> {
      val data =
        """
          |14555
          |16565
        """.stripMargin

      converter must not beNull
      val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList
      res.size must be equalTo 2
      res(0).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(55.0, 45.0)
      res(1).getDefaultGeometry.asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(65.0, 65.0)
    }
  }
}
