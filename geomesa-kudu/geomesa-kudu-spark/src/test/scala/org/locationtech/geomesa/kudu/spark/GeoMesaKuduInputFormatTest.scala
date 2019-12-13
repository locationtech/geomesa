/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.hadoop.mapreduce.Job
import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.kudu.spark.GeoMesaKuduInputFormat.GeoMesaKuduInputSplit
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaKuduInputFormatTest extends Specification {

  import scala.collection.JavaConverters._

  skipAll // integration

  val params = Map(
    "kudu.master"            -> "localhost",
    "kudu.catalog"           -> "geomesa",
    "geomesa.security.auths" -> "admin"
  )

  "GeoMesaKuduInputFormat" should {
    "create writable splits" in {
      val job = Job.getInstance()
      GeoMesaKuduInputFormat.configure(job.getConfiguration, params, new Query("testpoints"))

      val input = new GeoMesaKuduInputFormat()
      input.setConf(job.getConfiguration)

      val splits = input.getSplits(job).asScala
      splits must not(beEmpty)

      foreach(splits) { split =>
        val out = new ByteArrayOutputStream()
        split.asInstanceOf[GeoMesaKuduInputSplit].write(new DataOutputStream(out))
        out.flush()
        val in = new ByteArrayInputStream(out.toByteArray)
        val recovered = new GeoMesaKuduInputSplit()
        recovered.readFields(new DataInputStream(in))
        recovered mustEqual split
      }
    }
  }
}
