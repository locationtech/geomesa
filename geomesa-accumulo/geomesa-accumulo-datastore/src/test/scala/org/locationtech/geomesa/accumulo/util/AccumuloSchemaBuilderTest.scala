/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.stats.Cardinality
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloSchemaBuilderTest extends Specification {

  "AccumuloSchemaBuilder" should {
    "allow join indices" >> {
      val spec = AccumuloSchemaBuilder.builder()
          .addString("foo").withJoinIndex()
          .addInt("bar").withJoinIndex(Cardinality.HIGH)
          .spec

      spec mustEqual "foo:String:index=join,bar:Int:index=join:cardinality=high"
    }
  }
}
