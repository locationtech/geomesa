/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.nio.file.Files

import com.vividsolutions.jts.geom.Coordinate
import org.apache.hadoop.fs.Path
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class ParquetReadWriteTest extends Specification with AllExpectations {

  "SimpleFeatureParquetWriter" should {
    sequential
    val f = Files.createTempFile("test", ".parquet")
    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

    "write parquet files" >> {
      val writer = new SimpleFeatureParquetWriter(new Path(f.toUri), new SimpleFeatureWriteSupport(sft))

      val sf = new ScalaSimpleFeature("1", sft, Array("test", Integer.valueOf(100), new java.util.Date, gf.createPoint(new Coordinate(10, 10))))
      writer.write(sf)
      writer.close()
      Files.size(f) must be greaterThan 0
    }

    "read parquet files" >> {
      val reader = new SimpleFeatureParquetReader(new Path(f.toUri), new SimpleFeatureReadSupport(sft))
      val sf = reader.read()

      sf.getID must be equalTo "1"
      sf.getAttribute("name") must be equalTo "test"
    }
  }
}
