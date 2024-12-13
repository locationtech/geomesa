/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.pathfilter

import org.apache.hadoop.fs.Path
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.NamedOptions
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DtgPathFilteringTest extends Specification {

  import org.locationtech.geomesa.fs.storage.converter.pathfilter.DtgPathFiltering.Config._

  "DtgPathFilterFactory" should {
    "parse, format, buffer, and filter a dtg from a file path" in {
      val attribute = "dtg"
      val pattern = "^data-(.*)\\..*$"
      val format = "yyyyMMddHHmm"
      val buffer = "6 hours"
      val config = NamedOptions(DtgPathFiltering.Name,
        Map(Attribute -> attribute, Pattern -> pattern, Format -> format, Buffer -> buffer))

      val pathFilterFactory = PathFilteringFactory.load(config)
      pathFilterFactory must beSome { factory: PathFiltering =>
        factory must haveClass[DtgPathFiltering]
      }

      val filterText = s"$attribute DURING 2024-12-10T00:00:00Z/2024-12-11T00:00:00Z " +
        s"OR $attribute = 2024-12-10T00:00:00Z OR $attribute = 2024-12-11T00:00:00Z"
      val filter = ECQL.toFilter(filterText)
      val pathFilter = pathFilterFactory.get.apply(filter)

      val path1 = new Path("/geomesa/fs/data-202412080000.csv")
      val path2 = new Path("/geomesa/fs/data-202412092200.csv")
      val path3 = new Path("/geomesa/fs/data-202412110000.csv")
      val path4 = new Path("/geomesa/fs/data-202412110600.csv")
      val path5 = new Path("/geomesa/fs/data-202412111000.csv")

      pathFilter.accept(path1) must beFalse
      pathFilter.accept(path2) must beTrue
      pathFilter.accept(path3) must beTrue
      pathFilter.accept(path4) must beTrue
      pathFilter.accept(path5) must beFalse
    }
  }
}
