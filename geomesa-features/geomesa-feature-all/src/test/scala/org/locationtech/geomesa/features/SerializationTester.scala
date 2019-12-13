/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.text.SimpleDateFormat
import java.util.UUID

import org.locationtech.jts.geom.{Point, Polygon}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.avro.{AvroSimpleFeature, AvroSimpleFeatureWriter, FeatureSpecificReader}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.mutable.ListBuffer
import scala.util.Random

/*
 * Run with -Xms1024m -Xmx6000m -XX:MaxPermSize=1g to avoid GC issues
 */
object SerializationTester {

  def main(args: Array[String]) = {

    def createComplicatedFeatures(numFeatures : Int) : List[AvroSimpleFeature] = {
      val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date," +
          "*f7:Point:srid=4326,f8:Polygon:srid=4326,f9:List[Double],f10:Map[String,Int]"
      val sft = SimpleFeatureTypes.createType("test", geoSchema)
      val r = new Random()
      r.setSeed(0)

      val list = new ListBuffer[AvroSimpleFeature]
      for(i <- 0 until numFeatures){
        val fid = new FeatureIdImpl(r.nextString(5))
        val sf = new AvroSimpleFeature(fid, sft)

        sf.setAttribute("f0", r.nextString(10).asInstanceOf[Object])
        sf.setAttribute("f1", r.nextInt().asInstanceOf[Object])
        sf.setAttribute("f2", r.nextDouble().asInstanceOf[Object])
        sf.setAttribute("f3", r.nextFloat().asInstanceOf[Object])
        sf.setAttribute("f4", r.nextBoolean().asInstanceOf[Object])
        sf.setAttribute("f5", UUID.fromString("12345678-1234-1234-1234-123456789012"))
        sf.setAttribute("f6", new SimpleDateFormat("yyyyMMdd").parse("20140102"))
        sf.setAttribute("f7", GeohashUtils.wkt2geom("POINT(45.0 49.0)").asInstanceOf[Point])
        sf.setAttribute("f8", GeohashUtils.wkt2geom("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))").asInstanceOf[Polygon])
        sf.setAttribute("f9", List(r.nextDouble(), r.nextDouble(), r.nextDouble(), r.nextDouble(), r.nextDouble()))
        sf.setAttribute("f10", Map(r.nextString(10) -> r.nextInt(),
          r.nextString(10) -> r.nextInt(), r.nextString(10) -> r.nextInt(), r.nextString(10) -> r.nextInt()))
        list += sf
      }
      list.toList
    }

//    size avro: 1151750
//    size kryo: 1137102 1% smaller

//    encode avro: 6.565
//    encode kryo: 4.905 34% faster!

//    decode avro: 9.455
//    decode kryo: 7.625 24% faster!

//    encode/decode avro: 16.02
//    encode/decode kryo: 12.53 28% faster!

//    complex attributes size avro: 1301239
//    complex attributes size kryo: 1111239 17% smaller

//    complex attributes encode/decode avro: 30.980
//    complex attributes encode/decode kryo: 17.326 79% faster!

    val features = createComplicatedFeatures(5000)

    def two() = {
      val writer = new AvroSimpleFeatureWriter(features(0).getType)
      val reader = FeatureSpecificReader(features(0).getType)
      val baos = new ByteArrayOutputStream()
      var reusableEncoder: BinaryEncoder = null
      var reusableDecoder: BinaryDecoder = null
      features.map { f =>
        baos.reset()
        reusableEncoder = EncoderFactory.get().directBinaryEncoder(baos, reusableEncoder)
        writer.write(f, reusableEncoder)
        val bytes = baos.toByteArray
        reusableDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), reusableDecoder)
        val feat = reader.read(null, reusableDecoder)
        assert(f.getAttributes == feat.getAttributes)
        bytes.size
      }.sum
    }

    def three() = {
      val serializer = KryoFeatureSerializer(features(0).getType)
      features.map { f =>
        val bytes = serializer.serialize(f)
        val feat = serializer.deserialize(bytes)
        assert(f.getAttributes == feat.getAttributes)
        bytes.size
      }.sum
    }

    def time(runs: Int, f: () => Int) = {
      val start = System.currentTimeMillis()
      for(i <- 0 until runs) {
        f()
      }
      val end = System.currentTimeMillis()
      (end-start).toDouble/runs.toDouble
    }

    // prime
    println(two())
    println(three())

    val twos = time(1000, two)
    val threes = time(1000, three)

    println("avro: " + twos)
    println("kryo: " + threes)
    println()
  }
}
