/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.avro.AvroSimpleFeatureConverterFactory
import org.locationtech.geomesa.convert.fixedwidth.FixedWidthConverterFactory
import org.locationtech.geomesa.convert.jdbc.JdbcConverterFactory
import org.locationtech.geomesa.convert.json.JsonSimpleFeatureConverterFactory
import org.locationtech.geomesa.convert.simplefeature.SimpleFeatureSimpleFeatureConverterFactory
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory
import org.locationtech.geomesa.convert.xml.XMLConverterFactory
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FindConvertersTest extends Specification {

  "geomesa convert all" should {
    "register all the converters" >> {
      SimpleFeatureConverters.providers.map(_.getClass) must containAllOf(
        Seq(
          classOf[AvroSimpleFeatureConverterFactory],
          classOf[FixedWidthConverterFactory],
          classOf[DelimitedTextConverterFactory],
          classOf[XMLConverterFactory],
          classOf[JsonSimpleFeatureConverterFactory],
          classOf[CompositeConverterFactory[_]],
          classOf[JdbcConverterFactory],
          classOf[SimpleFeatureSimpleFeatureConverterFactory]
        )
      )
    }
  }

}
