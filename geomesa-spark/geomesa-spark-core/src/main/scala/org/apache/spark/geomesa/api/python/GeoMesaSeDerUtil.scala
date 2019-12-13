/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.geomesa.api.python

import java.io.OutputStream

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import net.razorvine.pickle.{IObjectPickler, Pickler}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.locationtech.geomesa.utils.text.WKBUtils

object GeoMesaSeDerUtil extends LazyLogging {

  implicit def toPickledRDD(rdd: RDD[_]): JavaRDD[Array[Byte]] =
    rdd.mapPartitions{ i =>  AutoBatchedPickler(i) }.toJavaRDD

  implicit def toPickledRDD(jrdd: JavaRDD[_]): JavaRDD[Array[Byte]] = toPickledRDD(jrdd.rdd)

}

object AutoBatchedPickler extends LazyLogging {

  val module = "geomesa_pyspark.types"
  val function = "_deserialize_from_wkb"

  val pickler = new IObjectPickler {
    def pickle(o: Object, os: OutputStream, p: Pickler): Unit = {
      import net.razorvine.pickle.Opcodes
      os.write(Opcodes.GLOBAL)
      os.write(s"$module\n$function\n".getBytes)
      p.save(WKBUtils.write(o.asInstanceOf[Geometry]))
      os.write(Opcodes.TUPLE1)
      os.write(Opcodes.REDUCE)
    }
  }
  Pickler.registerCustomPickler(classOf[Geometry], pickler)
  logger.info("registered JTS Geometry to WKB pickler")

  def apply(i: Iterator[Any]): AutoBatchedPickler =  new AutoBatchedPickler(i)
}

class AutoBatchedPickler(i: Iterator[Any]) extends SerDeUtil.AutoBatchedPickler(i)
