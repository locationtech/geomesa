/***********************************************************************
 * Copyright (c) 2018 Astraea, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.spark.jts

import java.util.concurrent.TimeUnit

import org.locationtech.geomesa.spark.jts.util.WKBUtils.WKBData
import org.locationtech.geomesa.spark.jts.util.{WKBDirectReader, WKBUtils, WKTUtils}
import org.openjdk.jmh.annotations._


@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
class WKBNumPointsBench {

  @Param(Array("POINT", "LINESTRING", "MULTIPOINT"))
  var testCase: String = _

  @transient
  var wkb: WKBData = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    val wkt = WKBNumPointsBench.testData(testCase)
    val geom = WKTUtils.read(wkt)
    wkb = WKBUtils.write(geom)
  }

  @Benchmark
  def deserializeNumPoints: Int = {
    val geom = WKBUtils.read(wkb)
    geom.getNumPoints
  }

  @Benchmark
  def directNumPoints: Int = {
    WKBDirectReader.getNumPoints(wkb)
  }
}

object WKBNumPointsBench {
  val testData = Map(
    "POINT" -> "POINT (30 10)",
    "LINESTRING" -> "LINESTRING (30 10, 10 30, 40 40)",
    "POLYGON_1" ->  "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
    "POLYGON_2" ->  "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))",
    "MULTIPOINT" -> "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
    "MULTILINESTRING" -> "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
    "MULTIPOLYGON" -> "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))"
  )
}
