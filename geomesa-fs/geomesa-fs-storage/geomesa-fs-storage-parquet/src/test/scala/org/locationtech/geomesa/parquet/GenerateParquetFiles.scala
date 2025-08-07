/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemContext
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage.ParquetFileSystemWriter
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose

import java.util.{Locale, UUID}

/**
 * Writes GeoParquet 'native' and 'wkb' files to /tmp/, for testing interoperability with other systems.
 */
object GenerateParquetFiles {

  def main(args: Array[String]): Unit = {

    val sft = SimpleFeatureTypes.createType("geoparquet-test",
      "dtg:Date,name:String,age:Int,time:Long,height:Float,weight:Double,bool:Boolean," +
        "uuid:UUID,bytes:Bytes,list:List[String],map:Map[String,Long]," +
        "line:LineString:srid=4326,multipt:MultiPoint:srid=4326,poly:Polygon:srid=4326," +
        "multiline:MultiLineString:srid=4326,multipoly:MultiPolygon:srid=4326,geom:Geometry:srid=4326," +
        "*pt:Point:srid=4326")

    val features = Seq.tabulate(10) { i =>
      val sf = new ScalaSimpleFeature(sft, i.toString)
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      sf.setAttribute("dtg", f"2014-01-${i + 1}%02dT00:00:01.000Z")
      sf.setAttribute("name", s"name$i")
      sf.setAttribute("age", s"$i")
      sf.setAttribute("time", s"$i")
      sf.setAttribute("height", s"$i")
      sf.setAttribute("weight", s"$i")
      sf.setAttribute("bool", Boolean.box(i < 5))
      sf.setAttribute("uuid", UUID.fromString(s"00000000-0000-0000-0000-00000000000$i"))
      sf.setAttribute("bytes", Array.tabulate[Byte](i)(i => i.toByte))
      sf.setAttribute("list", Seq.tabulate[String](i)(i => i.toString))
      sf.setAttribute("map", (0 until i).map(i => i.toString -> Long.box(i)).toMap)
      sf.setAttribute("line", s"LINESTRING(0 $i, 2 $i, 8 ${10 - i})")
      sf.setAttribute("multipt", s"MULTIPOINT(0 $i, 2 3)")
      sf.setAttribute("poly",
        if (i == 5) {
          // polygon example with holes from wikipedia
          "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))"
        } else {
          s"POLYGON((40 3$i, 42 3$i, 42 2$i, 40 2$i, 40 3$i))"
        }
      )
      sf.setAttribute("multiline", s"MULTILINESTRING((0 2, 2 $i, 8 6),(0 $i, 2 $i, 8 ${10 - i}))")
      sf.setAttribute("multipoly", s"MULTIPOLYGON(((-1 0, 0 $i, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6), (-1 5, 2 5, 2 2, -1 2, -1 5)))")
      sf.setAttribute("geom", sf.getAttribute(Seq("line", "multipt", "poly", "multiline", "multipoly").drop(i % 5).head))
      sf.setAttribute("pt", s"POINT(4$i 5$i)")
      sf
    }

    Seq(GeometryEncoding.GeoParquetNative, GeometryEncoding.GeoParquetWkb, GeometryEncoding.GeoMesaV1).foreach { encoding =>
      val config = new Configuration()
      config.set("parquet.compression", "gzip")
      config.set(SimpleFeatureParquetSchema.GeometryEncodingKey, encoding.toString)
      val dir = new Path(sys.props("java.io.tmpdir"))
      val file = new Path(dir, s"${encoding.toString.replace("GeoParquet", "geoparquet-").toLowerCase(Locale.US)}-test.parquet")
      val context = FileSystemContext(dir, config)
      WithClose(new ParquetFileSystemWriter(sft, context, file)) { writer =>
        features.foreach(writer.write)
      }
      println(s"Wrote ${features.length} features to $file")
    }
  }
}
