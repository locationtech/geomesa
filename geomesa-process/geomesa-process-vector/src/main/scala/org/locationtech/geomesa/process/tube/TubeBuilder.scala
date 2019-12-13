/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.tube

import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.referencing.GeodeticCalculator
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.NumericRange

object TubeBuilder {
  val DefaultDtgField = "dtg"
}

/**
 * Build a tube for input to a TubeSelect by buffering and binning the input
 * tubeFeatures into SimpleFeatures that can be used as inputs to Geomesa queries
 */
abstract class TubeBuilder(val tubeFeatures: SimpleFeatureCollection,
                           val bufferDistance: Double,
                           val maxBins: Int) extends LazyLogging {

  val calc = new GeodeticCalculator()
  val dtgField: String = tubeFeatures.getSchema.getDtgField.getOrElse(TubeBuilder.DefaultDtgField)
  val geoFac = new GeometryFactory

  val GEOM_PROP = "geom"

  val tubeType: SimpleFeatureType = SimpleFeatureTypes.createType("tubeType", s"$GEOM_PROP:Geometry:srid=4326,start:Date,end:Date")
  val builder: SimpleFeatureBuilder = ScalaSimpleFeatureFactory.featureBuilder(tubeType)

  def getGeom(sf: SimpleFeature): Geometry = sf.getAttribute(0).asInstanceOf[Geometry]
  def getStartTime(sf: SimpleFeature): Date = sf.getAttribute(1).asInstanceOf[Date]
  def getEndTime(sf: SimpleFeature): Date = sf.getAttribute(2).asInstanceOf[Date]

  def bufferGeom(geom: Geometry, meters: Double): Geometry = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    geom.buffer(metersToDegrees(meters, geom.safeCentroid()))
  }

  def metersToDegrees(meters: Double, point: Point): Double = {
    logger.debug("Buffering: "+meters.toString + " "+WKTUtils.write(point))

    calc.setStartingGeographicPoint(point.getX, point.getY)
    calc.setDirection(0, meters)
    val dest2D = calc.getDestinationGeographicPoint
    val destPoint = geoFac.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
    point.distance(destPoint)
  }

  def buffer(simpleFeatures: Iterator[SimpleFeature], meters:Double): Iterator[SimpleFeature] =
    simpleFeatures.map { sf =>
      val bufferedGeom = bufferGeom(getGeom(sf), meters)
      builder.reset()
      builder.init(sf)
      builder.set(GEOM_PROP, bufferedGeom)
      builder.buildFeature(sf.getID)
    }

  // transform the input tubeFeatures into the intermediate SF used by the
  // tubing code consisting of three attributes (geom, startTime, endTime)
  // handle date parsing from input -> TODO revisit date parsing...
  def transform(tubeFeatures: SimpleFeatureCollection, dtgField: String): Iterator[SimpleFeature] = {
    SelfClosingIterator(tubeFeatures.features).map { sf =>
      val date = FastConverter.convert(sf.getAttribute(dtgField), classOf[Date])

      if (date == null) {
        logger.error("Unable to retrieve date field from input tubeFeatures...ensure there a field named " + dtgField)
        throw new IllegalArgumentException("Unable to retrieve date field from input tubeFeatures...ensure there a field named \"" + dtgField + "\"")
      }

      builder.reset()
      builder.buildFeature(sf.getID, Array(sf.getDefaultGeometry, date, null))
    }
  }

  /**
    * Return an Array containing either 1 or 2 LineStrings that straddle but
    * do not cross the IDL.
    * @param input1 The first point in the segment
    * @param input2 The second point in the segment
    * @return an array of LineString containing either 1 or 2 LineStrings that do not
    *         span the IDL.
    */
  def makeIDLSafeLineString(input1:Coordinate, input2:Coordinate): Geometry = {
    //If the points cross the IDL we must generate two line segments
    if (GeometryUtils.crossesIDL(input1, input2)) {
      //Find the latitude where the segment intercepts the IDL
      val latIntercept = GeometryUtils.calcIDLIntercept(input1, input2)
      val p1 = new Coordinate(-180, latIntercept)
      val p2 = new Coordinate(180, latIntercept)
      //This orders the points so that point1 is always the east-most point
      val (point1, point2) = if (input1.x > 0) (input1, input2) else (input2, input1)
      val westLine = new LineString(new CoordinateArraySequence(Array(p1, point2)), geoFac)
      val eastLine = new LineString(new CoordinateArraySequence(Array(point1, p2)), geoFac)
      new MultiLineString(Array[LineString](westLine,eastLine), geoFac)
    } else {
      new LineString(new CoordinateArraySequence(Array(input1, input2)), geoFac)
    }
  }

  def createTube: Iterator[SimpleFeature]
}

/**
 * Build a tube with no gap filling - only buffering and binning
 */
class NoGapFill(tubeFeatures: SimpleFeatureCollection,
                bufferDistance: Double,
                maxBins: Int) extends TubeBuilder(tubeFeatures, bufferDistance, maxBins) with LazyLogging {

  // Bin ordered features into maxBins that retain order by date then union by geometry
  def timeBinAndUnion(features: Iterable[SimpleFeature], maxBins: Int): Iterator[SimpleFeature] = {
    val numFeatures = features.size

    if (numFeatures == 0) { Iterator.empty } else {
      //If 0 is passed in then don't bin the features, if 1 then make one bin, otherwise calculate number
      //of bins based on numFeatures and maxBins
      val binSize = maxBins match {
        case 0 => 1
        case 1 => numFeatures
        case _ => numFeatures / maxBins + (if (numFeatures % maxBins == 0 ) 0 else 1)
      }
      features.grouped(binSize).zipWithIndex.map { case(bin, idx) => unionFeatures(bin.toSeq, idx.toString) }
    }
  }

  // Union features to create a single geometry and single combined time range
  def unionFeatures(orderedFeatures: Seq[SimpleFeature], id: String): SimpleFeature = {
    import scala.collection.JavaConversions._
    val geoms = orderedFeatures.map { sf => getGeom(sf) }
    val unionGeom = geoFac.buildGeometry(geoms).union
    val min = getStartTime(orderedFeatures.head)
    val max = getStartTime(orderedFeatures.last)

    builder.reset()
    builder.buildFeature(id, Array(unionGeom, min, max))
  }

  override def createTube: Iterator[SimpleFeature] = {
    logger.debug("Creating tube with no gap filling")

    val transformed = transform(tubeFeatures, dtgField)
    val buffered = buffer(transformed, bufferDistance)
    val sortedTube = buffered.toSeq.sortBy { sf => getStartTime(sf).getTime }

    logger.debug(s"sorted tube size: ${sortedTube.size}")
    timeBinAndUnion(sortedTube, maxBins)
  }
}




/**
 * Build a tube with gap filling that draws a line between time-ordered features
 * from the given tubeFeatures
 */
class LineGapFill(tubeFeatures: SimpleFeatureCollection,
                  bufferDistance: Double,
                  maxBins: Int) extends TubeBuilder(tubeFeatures, bufferDistance, maxBins) with LazyLogging {

  val id = new AtomicInteger(0)

  def nextId: String = id.getAndIncrement.toString

  override def createTube: Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry

    logger.debug("Creating tube with line gap fill")

    val transformed = transform(tubeFeatures, dtgField)
    val sortedTube = transformed.toSeq.sortBy { sf => getStartTime(sf).getTime }
    val pointsAndTimes = sortedTube.map(sf => (getGeom(sf).safeCentroid(), getStartTime(sf)))
    val lineFeatures = if (pointsAndTimes.lengthCompare(1) == 0) {
      val (p1, t1) = pointsAndTimes.head
      logger.debug("Only a single result - can't create a line")
      Iterator(builder.buildFeature(nextId, Array(p1, t1, t1)))
    } else {
      pointsAndTimes.sliding(2).map { case Seq((p1, t1), (p2, t2)) =>
        val geo = if (p1.equals(p2)) p1 else makeIDLSafeLineString(p1.getCoordinate,p2.getCoordinate)
        logger.debug(s"Created Line-filled Geometry: ${WKTUtils.write(geo)} From ${WKTUtils.write(p1)} and ${WKTUtils.write(p2)}")
        builder.buildFeature(nextId, Array(geo, t1, t2))
      }
    }
    buffer(lineFeatures, bufferDistance)
  }
}

/**
  * Class to create an interpolated line-gap filled tube
  * @param tubeFeatures features
  * @param bufferDistance distance
  * @param maxBins max bins
  */
class InterpolatedGapFill(tubeFeatures: SimpleFeatureCollection,
                          bufferDistance: Double,
                          maxBins: Int) extends TubeBuilder(tubeFeatures, bufferDistance, maxBins) with LazyLogging {

  val id = new AtomicInteger(0)

  def nextId: String = id.getAndIncrement.toString

  override def createTube: Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry

    logger.debug("Creating tube with line interpolated line gap fill")

    val transformed = transform(tubeFeatures, dtgField)
    val sortedTube = transformed.toSeq.sortBy(sf => getStartTime(sf).getTime)
    val pointsAndTimes = sortedTube.map(sf => (getGeom(sf).safeCentroid(), getStartTime(sf)))
    val lineFeatures = if (pointsAndTimes.lengthCompare(1) == 0) {
      val (p1, t1) = pointsAndTimes.head
      logger.debug("Only a single result - can't create a line")
      Iterator(builder.buildFeature(nextId, Array(p1, t1, t1)))
    } else {
      pointsAndTimes.sliding(2).flatMap { case Seq((p1, t1), (p2, t2)) =>
        calc.setStartingGeographicPoint(p1.getX, p1.getY)
        calc.setDestinationGeographicPoint(p2.getX, p2.getY)
        val dist = calc.getOrthodromicDistance
        //If the distance between points is greater than the buffer distance, segment the line
        //So that no segment is larger than the buffer. This ensures that each segment has an
        //times and distance. Also ensure that features do not share a time value.
        val timeDiffMillis = toInstant(t2).toEpochMilli - toInstant(t1).toEpochMilli
        val segCount = (dist / bufferDistance).toInt
        val segDuration = timeDiffMillis / segCount
        val timeSteps = NumericRange.inclusive(toInstant(t1).toEpochMilli, toInstant(t2).toEpochMilli, segDuration)
        if (dist > bufferDistance && timeSteps.lengthCompare(1) > 0) {
          val heading = calc.getAzimuth
          var segStep = new Coordinate(p1.getX, p1.getY, 0)
          timeSteps.sliding(2).map { case Seq(time0, time1) =>
            val segP1 = segStep
            calc.setStartingGeographicPoint(segP1.x, segP1.y)
            calc.setDirection(heading, bufferDistance)
            val destPoint = calc.getDestinationGeographicPoint
            segStep = new Coordinate(destPoint.getX, destPoint.getY, 0)
            val geo = makeIDLSafeLineString(segP1, segStep)
            builder.buildFeature(nextId, Array(geo, new Date(time0), new Date(time1)))
          }
        } else {
          val geo = if (p1.equals(p2)) { p1 } else { makeIDLSafeLineString(p1.getCoordinate, p2.getCoordinate) }
          logger.debug(s"Created line-filled geometry: ${WKTUtils.write(geo)} " +
              s"from ${WKTUtils.write(p1)} and ${WKTUtils.write(p2)}")
          Seq(builder.buildFeature(nextId, Array(geo, t1, t2)))
        }
      }
    }
    buffer(lineFeatures, bufferDistance)
  }
}
