/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.util.Date

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.{AbstractGridFormat, StructuredGridCoverage2DReader}
import org.geotools.data.Query
import org.geotools.geometry.DirectPosition2D
import org.geotools.parameter.Parameter
import org.geotools.util.DateRange
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.coverage.PointOutsideCoverageException
import org.opengis.coverage.grid.GridCoverageReader
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.parameter.GeneralParameterValue

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Code for evaluating coverages at a given spatio-temporal point.
  */
class CoverageEvaluator(covReader: GridCoverageReader) extends LazyLogging {
  val coverageNames = covReader.getGridCoverageNames

  logger.debug(s" *** Here: coveragenames are ${coverageNames.mkString(", ")}")

  val granuleTimes = getGranuleTimes

  logger.debug(s"  **** with time granules $granuleTimes")

  // TODO: Expose coverage cache configuration
  val cacheMaxSize = 500
  lazy val gridCoverageCache: LoadingCache[Date, Seq[GridCoverage2D]] = buildGridCoverageCache(cacheMaxSize)
  lazy val gridCoverageNull  = getGridCoverageNull

  /**
    * Get a list of times associated with each granule in the coverage reader.
    */
  def getGranuleTimes(): List[Date] = {
    coverageNames.headOption match {
      case Some(name) => {
        covReader match {
          case structReader: StructuredGridCoverage2DReader => {
            val granuleSource = structReader.getGranules(name, true)
            Option(granuleSource) match {
              case Some(src) => {
                val sfc = src.getGranules(new Query(null, Filter.INCLUDE))
                SelfClosingIterator(sfc.features).map(sf => sf.getAttribute("time").asInstanceOf[Date]).toList.sorted
              }
              case None => List()
            }
          }
          case _ => List()
        }
      }
      case None => List()
    }
  }

  /**
    * Evaluate coverage at given lat, long, and datetime.
    */
  def evalAtPoint(lon: Double, lat: Double, dt: Date, sft: SimpleFeatureType): List[(Int, Double)] = {
    val covs = getGridCoverage(dt)

    val values: Seq[(Int, Double)] = for {
      cov <- covs
      ret = evalCovAtPoint(cov, lon, lat, dt) if ret.nonEmpty

    } yield {
      sft.indexOf(cov.getName.toString()) -> ret(0)
    }


//    val crs = cov.getCoordinateReferenceSystem2D
//    val pos = new DirectPosition2D(crs, lon, lat)
//
//
//    val values = Try(convertToDouble(cov.evaluate(pos))) match {
//      case Success(a: Array[Double]) => a
//      case Failure(ex: PointOutsideCoverageException) =>
//        println(s"For ($lon, $lat, $dt) Point was outside of coverage")
//        Array()
//      case Failure(ex) => throw ex
//    }

//    val ret = coverageNames.toList.map(sft.indexOf).zip(values)
    logger.trace(s" Queried at ($lon, $lat, $dt) and got back ${values.mkString(", ")}")
    values.toList
  }

  def evalCovAtPoint(cov: GridCoverage2D, lon: Double, lat: Double, dt: Date): Array[Double] = {
    val crs = cov.getCoordinateReferenceSystem2D

    // Netcdfs are set for 0 to 360.
    val adjLon = if (lon < 0) {
      lon + 360
    } else {
      lon
    }

    val pos = new DirectPosition2D(crs, adjLon, lat)


    val values = Try(convertToDouble(cov.evaluate(pos))) match {
      case Success(a: Array[Double]) => a
      case Failure(ex: PointOutsideCoverageException) =>
        logger.trace(s"For ($lon, $lat, $dt) Point was outside of coverage. Envelope is ${cov.getEnvelope}")
        Array[Double]()
      case Failure(ex) => throw ex
    }
    values
  }

  /**
    * Snap to most recent datetime granule (defaulting to first granule
    * if nothing found)
    */
  def snapTimeToGranule(dt: Date) =
    granuleTimes.filter(_.compareTo(dt) <= 0).lastOption match {
      case Some(t) =>
        logger.trace(s" For Date $dt returning time $t")
        t
      case None => {
        logger.trace(s" For Date $dt returning first granule.")
        granuleTimes.head
      }
    }

  /**
    * Get and "cache" the default GridCoverage2D from covReader.
    */
  def getGridCoverageNull = {
    logger.info(s"Getting null grid coverage")
    covReader.read(null).asInstanceOf[GridCoverage2D]
  }

  /**
    * Build a cache of GridCoverage2D objects at particular time
    * granules from covReader.
    */
  def buildGridCoverageCache(maxSize: Int): LoadingCache[Date, Seq[GridCoverage2D]] = {
    val cacheBuilder = CacheBuilder.newBuilder().maximumSize(maxSize)

    cacheBuilder.build(new CacheLoader[Date, Seq[GridCoverage2D]] {
      // we don't do checking for null or snapping to granule here because
      // that's handled by the upstream getGridCoverage() method
      override def load(dt: Date): Seq[GridCoverage2D] = {
        logger.info(s"Getting grid coverage at dt=$dt")
        val timeParam = new Parameter(AbstractGridFormat.TIME)
        timeParam.setValue(List(new DateRange(dt, dt)).asJava)
        val p: Array[GeneralParameterValue] = Array(timeParam)
        coverageNames.map {
          name =>
            covReader.read(name, p).asInstanceOf[GridCoverage2D]
        }
      }
    })
  }

  /**
    * Get a GridCoverage2D for the given (granule) date
    */
  def getGridCoverage(dt: Date): Seq[GridCoverage2D] = {
    if (granuleTimes.isEmpty || dt == null) {
      Seq(gridCoverageNull)
    }
    else {
      val dtGranule = snapTimeToGranule(dt)
      gridCoverageCache.get(dtGranule)
    }
  }

  /**
    * Convert Array[T] to Array[Double]
    */
  def convertToDouble[T]: PartialFunction[Any, Array[Double]] = {
    case a: Array[Int] => a.map(_.toDouble)
    case a: Array[Double] => a
    case a: Array[Float] => a.map(_.toDouble)
    case a: Array[Byte] => a.map(x => {
      val b = x.toDouble
      if (b < 0) 256.0 + b
      else b
      b.toDouble
    })
    case _ => throw new Exception("expected an double, int, or float, or byte")
  }
}
