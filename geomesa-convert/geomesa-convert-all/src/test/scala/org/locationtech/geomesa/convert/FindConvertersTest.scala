/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.util.ServiceLoader

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.avro.{AvroConverter, AvroConverterFactory}
import org.locationtech.geomesa.convert.fixedwidth.{FixedWidthConverter, FixedWidthConverterFactory}
import org.locationtech.geomesa.convert.jdbc.{JdbcConverter, JdbcConverterFactory}
import org.locationtech.geomesa.convert.json.{JsonConverter, JsonConverterFactory}
import org.locationtech.geomesa.convert.text.{DelimitedTextConverter, DelimitedTextConverterFactory}
import org.locationtech.geomesa.convert.xml.{XmlConverter, XmlConverterFactory}
import org.locationtech.geomesa.convert2
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FindConvertersTest extends Specification {

  "geomesa convert all" should {

    "find all classes for converters" >> {
      classOf[AvroConverter] must not(throwAn[ClassNotFoundException])
      classOf[AvroConverterFactory] must not(throwAn[ClassNotFoundException])

      classOf[FixedWidthConverter] must not(throwAn[ClassNotFoundException])
      classOf[FixedWidthConverterFactory] must not(throwAn[ClassNotFoundException])

      classOf[JsonConverter] must not(throwAn[ClassNotFoundException])
      classOf[JsonConverterFactory] must not(throwAn[ClassNotFoundException])

      classOf[DelimitedTextConverter] must not(throwAn[ClassNotFoundException])
      classOf[DelimitedTextConverterFactory] must not(throwAn[ClassNotFoundException])

      classOf[XmlConverter] must not(throwAn[ClassNotFoundException])
      classOf[XmlConverterFactory] must not(throwAn[ClassNotFoundException])

      classOf[org.locationtech.geomesa.convert.CompositeConverter[_]] must not(throwAn[ClassNotFoundException])
      classOf[org.locationtech.geomesa.convert.CompositeConverterFactory[_]] must not(throwAn[ClassNotFoundException])

      classOf[org.locationtech.geomesa.convert2.composite.CompositeConverter] must not(throwAn[ClassNotFoundException])
      classOf[org.locationtech.geomesa.convert2.composite.CompositeConverterFactory] must not(throwAn[ClassNotFoundException])

      classOf[JdbcConverter] must not(throwA[ClassNotFoundException])
      classOf[JdbcConverterFactory] must not(throwA[ClassNotFoundException])

      classOf[SimpleFeatureConverterFactory[_]] must not(throwAn[ClassNotFoundException])
    }

    "register all the converters" >> {
      import scala.collection.JavaConverters._

      ServiceLoader.load(classOf[convert2.SimpleFeatureConverterFactory]).asScala.map(_.getClass) must containAllOf(
        Seq(
          classOf[org.locationtech.geomesa.convert2.composite.CompositeConverterFactory],
          classOf[AvroConverterFactory],
          classOf[FixedWidthConverterFactory],
          classOf[DelimitedTextConverterFactory],
          classOf[XmlConverterFactory],
          classOf[JsonConverterFactory],
          classOf[JdbcConverterFactory]
        )
      )
    }

  }
}
