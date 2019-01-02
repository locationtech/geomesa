/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import java.util.{Date, UUID}

import org.locationtech.jts.geom.{Geometry, Point}
import org.apache.kudu.Schema
import org.apache.kudu.client.RowResult
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KuduColumnAdapterTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test",
    "string:String,int:Int,long:Long,float:Float,double:Double,boolean:Boolean,date:Date,uuid:UUID," +
        "bytes:Bytes,list:List[Float],map:Map[String,Int],*point:Point:srid=4326,line:LineString:srid=4326")

  "KuduColumnAdapter" should {
    "adapt string columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("string")).asInstanceOf[KuduColumnAdapter[String]]

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, "test")
      write.getString(adapter.columns.head.getName) mustEqual "test"

      val read = mock[RowResult]
      read.getString(adapter.columns.head.getName) returns "test"
      adapter.readFromRow(read) mustEqual "test"
    }

    "adapt int columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("int")).asInstanceOf[KuduColumnAdapter[Int]]

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, 2)
      write.getInt(adapter.columns.head.getName) mustEqual 2

      val read = mock[RowResult]
      read.getInt(adapter.columns.head.getName) returns 2
      adapter.readFromRow(read) mustEqual 2
    }

    "adapt long columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("long")).asInstanceOf[KuduColumnAdapter[Long]]

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, 3L)
      write.getLong(adapter.columns.head.getName) mustEqual 3L

      val read = mock[RowResult]
      read.getLong(adapter.columns.head.getName) returns 3L
      adapter.readFromRow(read) mustEqual 3L
    }

    "adapt float columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("float")).asInstanceOf[KuduColumnAdapter[Float]]

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, 4.1f)
      write.getFloat(adapter.columns.head.getName) mustEqual 4.1f

      val read = mock[RowResult]
      read.getFloat(adapter.columns.head.getName) returns 4.1f
      adapter.readFromRow(read) mustEqual 4.1f
    }

    "adapt double columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("double")).asInstanceOf[KuduColumnAdapter[Double]]

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, 5.2d)
      write.getDouble(adapter.columns.head.getName) mustEqual 5.2d

      val read = mock[RowResult]
      read.getDouble(adapter.columns.head.getName) returns 5.2d
      adapter.readFromRow(read) mustEqual 5.2d
    }

    "adapt boolean columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("boolean")).asInstanceOf[KuduColumnAdapter[Boolean]]

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, true)
      write.getBoolean(adapter.columns.head.getName) mustEqual true

      val read = mock[RowResult]
      read.getBoolean(adapter.columns.head.getName) returns true
      adapter.readFromRow(read) mustEqual true
    }

    "adapt date columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("date")).asInstanceOf[KuduColumnAdapter[Date]]

      val date = Converters.convert("2018-01-02T00:01:30.000Z", classOf[Date])
      date must not(beNull)

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, date)
      write.getLong(adapter.columns.head.getName) mustEqual date.getTime * 1000 // micros

      val read = mock[RowResult]
      read.getLong(adapter.columns.head.getName) returns date.getTime * 1000
      adapter.readFromRow(read) mustEqual date
    }

    "adapt uuid columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("uuid")).asInstanceOf[KuduColumnAdapter[UUID]]

      val uuid = UUID.fromString("e8c274ef-7d7c-4556-af68-1fc4b7f26a36")

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, uuid)
      val bytes = write.getBinary(adapter.columns.head.getName)
      bytes.remaining() mustEqual 16
      bytes.getLong mustEqual uuid.getMostSignificantBits
      bytes.getLong mustEqual uuid.getLeastSignificantBits
      bytes.rewind()

      val read = mock[RowResult]
      read.getBinary(adapter.columns.head.getName) returns bytes
      adapter.readFromRow(read) mustEqual uuid
    }

    "adapt bytes columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("bytes")).asInstanceOf[KuduColumnAdapter[Array[Byte]]]

      val bytes = Array.tabulate[Byte](7)(i => i.toByte)

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, bytes)
      write.getBinaryCopy(adapter.columns.head.getName) mustEqual bytes

      val read = mock[RowResult]
      read.getBinaryCopy(adapter.columns.head.getName) returns bytes
      adapter.readFromRow(read) mustEqual bytes
    }

    "adapt list columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("list")).asInstanceOf[KuduColumnAdapter[java.util.List[Float]]]

      val list = Seq(0f, 1f, 2f).asJava

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, list)

      val read = mock[RowResult]
      read.getBinary(adapter.columns.head.getName) returns write.getBinary(adapter.columns.head.getName)
      adapter.readFromRow(read) mustEqual list
    }

    "adapt map columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("map")).asInstanceOf[KuduColumnAdapter[java.util.Map[String, Int]]]

      val map = Map("zero" -> 0, "one" -> 1, "two" -> 2).asJava

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, map)

      val read = mock[RowResult]
      read.getBinary(adapter.columns.head.getName) returns write.getBinary(adapter.columns.head.getName)
      adapter.readFromRow(read) mustEqual map
    }

    "adapt point columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("point")).asInstanceOf[KuduColumnAdapter[Point]]

      val pt = WKTUtils.read("POINT(45 55)").asInstanceOf[Point]

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, pt)
      write.getDouble(adapter.columns.head.getName) mustEqual 45d
      write.getDouble(adapter.columns.last.getName) mustEqual 55d

      val read = mock[RowResult]
      read.getDouble(adapter.columns.head.getName) returns 45d
      read.getDouble(adapter.columns.last.getName) returns 55d
      adapter.readFromRow(read) mustEqual pt
    }

    "adapt non-point columns" in {
      val adapter = KuduColumnAdapter(sft, sft.getDescriptor("line")).asInstanceOf[KuduColumnAdapter[Geometry]]

      val line = WKTUtils.read("LINESTRING(45 55, 46 56, 47 57)")

      val write = new Schema(adapter.writeColumns.asJava).newPartialRow()
      adapter.writeToRow(write, line)

      val read = mock[RowResult]
      read.getBinary(adapter.columns.head.getName) returns write.getBinary(adapter.columns.head.getName)
      adapter.readFromRow(read) mustEqual line
    }
  }
}
