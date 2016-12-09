/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.accumulo

import com.typesafe.config.ConfigFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

object GeoNames {

  val conf = ConfigFactory.parseString(
    """
      | {
      |      name = "geonames-tsv"
      |      type = "delimited-text",
      |      format = "TSV",
      |      id-field = "toString($geonameid)",
      |      fields = [
      |        { name = "geonameid",           transform = "try($1::int, null)" },
      |        { name = "name",                transform = "$2::string" },
      |        { name = "asciiname",           transform = "$3::string" },
      |        { name = "lat",                 transform = "try($5::double, null)" },
      |        { name = "long",                transform = "try($6::double, null)" },
      |        { name = "geom",                transform = "point($long, $lat)" }
      |
      |        { name = "featureclass",        transform = "$7::string" },
      |        { name = "featurecode",         transform = "$8::string" },
      |        { name = "countrycode",         transform = "$9::string" },
      |        { name = "cc2",                 transform = "$10::string" },
      |        { name = "admin1code",          transform = "$11::string" },
      |        { name = "admin2code",          transform = "$12::string" },
      |        { name = "admin3code",          transform = "$13::string" },
      |        { name = "admin4code",          transform = "$14::string" },
      |        { name = "population",          transform = "try($15::long, null)" },
      |        { name = "elevation",           transform = "try($16::int, null)" },
      |        { name = "dem",                 transform = "try($17::int, null)" },
      |        { name = "timezone",            transform = "$18::string" },
      |        { name = "modificationdate",    transform = "date('YYYY-MM-dd', $19)" },
      |      ]
      |    }
      |
    """.stripMargin)

  val sft = SimpleFeatureTypes.createType(ConfigFactory.parseString(
    """
      |  {
      |      type-name = "geonames"
      |      attributes = [
      |        { name = "geonameid", type = "Integer", index = false },
      |        { name = "name", type = "String", index = true },
      |        { name = "asciiname", type = "String", index = false },
      |
      |        { name = "featureclass", type = "String", index = false },
      |        { name = "featurecode", type = "String", index = false },
      |        { name = "countrycode", type = "String", index = false },
      |        { name = "cc2", type = "String", index = false },
      |        { name = "admin1code", type = "String", index = false },
      |        { name = "admin2code", type = "String", index = false },
      |        { name = "admin3code", type = "String", index = false },
      |        { name = "admin4code", type = "String", index = false },
      |        { name = "population", type = "Long", index = false },
      |        { name = "elevation", type = "Integer", index = false },
      |        { name = "dem", type = "Integer", index = false },
      |        { name = "timezone", type = "String", index = false },
      |        { name = "modificationdate", type = "Date", index = false },
      |        { name = "geom", type = "Point", index = true, srid = 4326, default = true }
      |      ]
      |    }
    """.stripMargin))


}
