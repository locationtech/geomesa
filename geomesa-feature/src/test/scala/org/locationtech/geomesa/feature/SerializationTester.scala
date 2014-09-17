/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.feature

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.UUID

import com.vividsolutions.jts.geom.{Point, Polygon}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.mutable.ListBuffer
import scala.util.Random

/*
 * Run with -Xms1024m -Xmx6000m -XX:MaxPermSize=1g to avoid GC issues
 */
object SerializationTester {

  def main(args: Array[String]) = {

    def createComplicatedFeatures(numFeatures : Int) : List[Version2ASF] = {
      val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326"
      val sft = SimpleFeatureTypes.createType("test", geoSchema)
      val r = new Random()
      r.setSeed(0)


      val list = new ListBuffer[Version2ASF]
      for(i <- 0 until numFeatures){
        val fid = new FeatureIdImpl(r.nextString(5))
        val sf = new Version2ASF(fid, sft)

        sf.setAttribute("f0", r.nextString(10).asInstanceOf[Object])
        sf.setAttribute("f1", r.nextInt().asInstanceOf[Object])
        sf.setAttribute("f2", r.nextDouble().asInstanceOf[Object])
        sf.setAttribute("f3", r.nextFloat().asInstanceOf[Object])
        sf.setAttribute("f4", r.nextBoolean().asInstanceOf[Object])
        sf.setAttribute("f5", UUID.fromString("12345678-1234-1234-1234-123456789012"))
        sf.setAttribute("f6", new SimpleDateFormat("yyyyMMdd").parse("20140102"))
        sf.setAttribute("f7", GeohashUtils.wkt2geom("POINT(45.0 49.0)").asInstanceOf[Point])
        sf.setAttribute("f8", GeohashUtils.wkt2geom("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))").asInstanceOf[Polygon])
        list += sf
      }
      list.toList
    }

    val features = createComplicatedFeatures(500000)

    def one() = {
      val oldBaos = new ByteArrayOutputStream()
      features.foreach { f =>
        oldBaos.reset()
        f.write(oldBaos)
        oldBaos.toByteArray
      }
    }

    def two() = {
      val writer = new AvroSimpleFeatureWriter(features(0).getType)
      val baos = new ByteArrayOutputStream()
      var reusableEncoder: BinaryEncoder = null
      features.foreach { f =>
        baos.reset()
        reusableEncoder = EncoderFactory.get().directBinaryEncoder(baos, reusableEncoder)
        writer.write(f, reusableEncoder)
        baos.toByteArray
      }
    }

    def time(runs: Int, f: () => Unit) = {
      val start = System.currentTimeMillis()
      for(i <- 0 until runs) {
        f()
      }
      val end = System.currentTimeMillis()
      (end-start).toDouble/runs.toDouble
    }
    // prime
    one
    two

    val ones = time(100, one)
    val twos = time(100, two)

    println("1: " + ones)
    println("2: " + twos)
    println("r: " + ones/twos)

  }
}
