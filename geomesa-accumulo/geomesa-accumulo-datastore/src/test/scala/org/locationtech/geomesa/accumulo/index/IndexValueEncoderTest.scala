/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, UUID}

import org.locationtech.jts.geom.Geometry
import org.apache.accumulo.core.data.Value
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexValueEncoderTest extends Specification {

  import scala.collection.JavaConverters._

  val defaultSchema = "*geom:Point,dtg:Date,s:String,i:Int,d:Double,f:Float,u:UUID,l:List[String]"
  val allSchema = s"*geom:Point:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,s:String:$OPT_INDEX_VALUE=true,i:Int:$OPT_INDEX_VALUE=true,d:Double:$OPT_INDEX_VALUE=true,f:Float:$OPT_INDEX_VALUE=true,u:UUID:$OPT_INDEX_VALUE=true,l:List[String]"
  val id = "Feature0123456789"
  val geom = WKTUtils.read("POINT (-78.495356 38.075215)")
  val dt = new Date()

  // b/c the IndexValueEncoder caches feature types, we need to change the sft name for each test
  val index = new AtomicInteger(0)
  def getSft(schema: String = defaultSchema) =
    SimpleFeatureTypes.createType("IndexValueEncoderTest" + index.getAndIncrement, schema)

  def getIndexValueFields(sft: SimpleFeatureType): Seq[String] =
    IndexValueEncoder.getIndexSft(sft).getAttributeDescriptors.asScala.map(_.getLocalName)

  "IndexValueEncoder" should {
    "default to id,geom,date" in {
      val sft = getSft()
      getIndexValueFields(sft) must containAllOf(Seq("geom", "dtg"))
    }
    "default to id,geom if no date" in {
      val sft = getSft("*geom:Point,foo:String")
      getIndexValueFields(sft) must containAllOf(Seq("geom"))
    }
    "allow custom fields to be set" in {
      val sft = getSft(s"*geom:Point:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,s:String,i:Int:$OPT_INDEX_VALUE=true,d:Double,f:Float:$OPT_INDEX_VALUE=true,u:UUID,l:List[String]")
      getIndexValueFields(sft) must containAllOf(Seq("geom", "dtg", "i", "f"))
    }
    "always include id,geom,dtg" in {
      val sft = getSft(s"*geom:Point,dtg:Date,s:String,i:Int:$OPT_INDEX_VALUE=true,d:Double,f:Float:$OPT_INDEX_VALUE=true,u:UUID,l:List[String]")
      getIndexValueFields(sft) must containAllOf(Seq("geom", "dtg", "i", "f"))
    }
    "not allow complex types" in {
      val sft = getSft(s"*geom:Point:$OPT_INDEX_VALUE=true,dtg:Date:$OPT_INDEX_VALUE=true,l:List[String]:$OPT_INDEX_VALUE=true")
      getIndexValueFields(sft) must containAllOf(Seq("geom", "dtg"))
    }

    "encode and decode id,geom,date" in {
      val sft = getSft()

      // inputs
      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.serialize(entry)

      // requirements
      value must not(beNull)

      // return trip
      val decoded = encoder.deserialize(value)

      // requirements
      decoded must not(beNull)
      decoded.getAttributeCount mustEqual 2
      decoded.getID mustEqual ""
      decoded.getAttribute("geom") mustEqual geom
      decoded.getAttribute("dtg") mustEqual dt
    }

    "encode and decode id,geom,date when there is no date" in {
      val sft = getSft()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, null, null, null, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.serialize(entry)

      // requirements
      value must not(beNull)

      // return trip
      val decoded = encoder.deserialize(value)

      // requirements
      decoded must not(beNull)
      decoded.getAttribute("dtg") must beNull
      decoded.getAttribute("geom") mustEqual geom
      decoded.getID mustEqual ""
    }

    "encode and decode custom fields" in {
      val sft = getSft(allSchema)

      val s = "test"
      val i: java.lang.Integer = 5
      val d: java.lang.Double = 10d
      val f: java.lang.Float = 1.0f
      val u = UUID.randomUUID()

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, dt, s, i, d, f, u, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.serialize(entry)

      // requirements
      value must not(beNull)

      // return trip
      val decoded = encoder.deserialize(value)

      // requirements
      decoded must not(beNull)
      decoded.getAttributeCount mustEqual 7
      decoded.getAttribute("geom") mustEqual geom
      decoded.getID mustEqual ""
      decoded.getAttribute("dtg") mustEqual dt
      decoded.getAttribute("d") mustEqual d
      decoded.getAttribute("f") mustEqual f
      decoded.getAttribute("i") mustEqual i
      decoded.getAttribute("s") mustEqual s
      decoded.getAttribute("u") mustEqual u
    }

    "encode and decode null values" in {
      val sft = getSft(allSchema)

      val i: java.lang.Integer = 5
      val d: java.lang.Double = 10d
      val f: java.lang.Float = 1.0f

      val entry = AvroSimpleFeatureFactory.buildAvroFeature(sft,
        List(geom, null, null, i, d, f, null, null), id)

      val encoder = IndexValueEncoder(sft)

      // output
      val value = encoder.serialize(entry)

      // requirements
      value must not(beNull)

      // return trip
      val decoded = encoder.deserialize(value)

      // requirements
      decoded must not(beNull)
      decoded.getAttributeCount mustEqual 7
      decoded.getAttribute("geom") mustEqual geom
      decoded.getID mustEqual ""
      decoded.getAttribute("d") mustEqual d
      decoded.getAttribute("f") mustEqual f
      decoded.getAttribute("i") mustEqual i
      decoded.getAttribute("s") must beNull
      decoded.getAttribute("u") must beNull
      decoded.getAttribute("dtg") must beNull
    }
  }

  // FOLLOWING METHODS ARE THE OLD ONES FROM IndexEntry

  // the index value consists of the feature's:
  // 1.  ID
  // 2.  WKB-encoded geometry
  // 3.  start-date/time
  def _encodeIndexValue(entry: SimpleFeature): Value = {
    val encodedId = entry.getID.getBytes
    val encodedGeom = WKBUtils.write(entry.getDefaultGeometry.asInstanceOf[Geometry])
    // dtg prop is hard-coded here for convenience
    val encodedDtg = Option(entry.getAttribute("dtg").asInstanceOf[Date])
        .map(dtg => ByteBuffer.allocate(8).putLong(dtg.getTime).array()).getOrElse(Array[Byte]())

    new Value(
      ByteBuffer.allocate(4).putInt(encodedId.length).array() ++ encodedId ++
          ByteBuffer.allocate(4).putInt(encodedGeom.length).array() ++ encodedGeom ++
          encodedDtg)
  }

  def _decodeIndexValue(v: Value): _DecodedIndexValue = {
    val buf = v.get()
    val idLength = ByteBuffer.wrap(buf, 0, 4).getInt
    val (idPortion, geomDatePortion) = buf.drop(4).splitAt(idLength)
    val id = new String(idPortion)
    val geomLength = ByteBuffer.wrap(geomDatePortion, 0, 4).getInt
    if(geomLength < (geomDatePortion.length - 4)) {
      val (l,r) = geomDatePortion.drop(4).splitAt(geomLength)
      _DecodedIndexValue(id, WKBUtils.read(l), Some(ByteBuffer.wrap(r).getLong))
    } else {
      _DecodedIndexValue(id, WKBUtils.read(geomDatePortion.drop(4)), None)
    }
  }

  case class _DecodedIndexValue(id: String, geom: Geometry, dtgMillis: Option[Long])
}
