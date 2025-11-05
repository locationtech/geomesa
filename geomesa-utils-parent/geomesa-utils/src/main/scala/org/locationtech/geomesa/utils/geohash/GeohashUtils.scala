/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.locationtech.spatial4j.context.jts.JtsSpatialContext

import scala.annotation.tailrec
import scala.collection.BitSet
import scala.collection.immutable.Range.Inclusive
import scala.util.Try
import scala.util.control.Exception.catching

/**
 * The following bits of code are related to common operations involving
 * GeoHashes, such as recommending a GeoHash precision for an enclosing
 * polygon; decomposing a polygon into a fixed number of subordinate
 * GeoHashes; enumerating possible sub-strings within subordinate GeoHashes
 * for a given polygon; etc.
 */
object GeohashUtils extends LazyLogging {

  // make sure the implicits related to distance are in-scope
  import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry

  /**
   * Simple place-holder for a pair of resolutions, minimum and maximum, along
   * with an increment.
   *
   * @param minBitsResolution minimum number of bits (GeoHash) resolution to consider; must be ODD
   * @param maxBitsResolution maximum number of bits (GeoHash) resolution to consider; must be ODD
   */
  case class ResolutionRange(minBitsResolution:Int=5,
                             maxBitsResolution:Int=63,
                             numBitsIncrement:Int=2) {
    // validate resolution arguments
    if (minBitsResolution >= maxBitsResolution)
      throw new IllegalArgumentException("Minimum resolution must be strictly greater than maximum resolution.")

    lazy val range: Range = new Inclusive(minBitsResolution, maxBitsResolution, numBitsIncrement)
    override def toString: String = "{" + minBitsResolution.toString + ", +" + numBitsIncrement + "..., " + maxBitsResolution.toString + "}"

    private def getNumChildren: Int = 1 << numBitsIncrement

    def getNextChildren(parent:BitSet, oldPrecision:Int) : List[BitSet] =
      Range(0, getNumChildren).map { i =>
        // compute bit-set corresponding to this integer,
        // and add it to the left-shifted version of the parent
        val bitString = i.toBinaryString

        Range(0, numBitsIncrement).foldLeft(parent) { case (bs, j) =>
          val c = if (j < bitString.length) bitString.charAt(j) else '0'
          if (c == '1') bs + (oldPrecision + bitString.length - 1 - j)
          else bs
        }
      }.toList

  }

  // default precision model
  private val maxRealisticGeoHashPrecision : Int = 45
  private val numDistinctGridPoints: Long = 1L << ((maxRealisticGeoHashPrecision+1)/2).toLong
  private val defaultPrecisionModel = new PrecisionModel(numDistinctGridPoints.toDouble)

  // default factory for WGS84
  private val defaultGeometryFactory : GeometryFactory = new GeometryFactory(defaultPrecisionModel, 4326)

  /**
   * Utility function that computes the minimum-bounding GeoHash that completely
   * encloses the given (target) geometry.  This can be useful as a starting
   * point for other calculations on the geometry.
   *
   * This is a bit of a short-cut, because what it *really* does is to start
   * with the centroid of the target geometry, and step down through resolutions
   * (in bits) for as long as a GeoHash constructed for that centroid continues
   * to enclose the entire geometry.  This works, because any GeoHash that
   * encloses the entire geometry must also include the centroid (as well as
   * any other point).
   *
   * @param geom the target geometry that is to be enclosed
   * @return the smallest GeoHash -- at an odd number of bits resolution -- that
   *         completely encloses the target geometry
   */
  private def getMinimumBoundingGeohash(geom:Geometry, resolutions:ResolutionRange) : GeoHash = {
    // save yourself some effort by computing the geometry's centroid and envelope up front
    val centroid = getCentroid(geom)
    val env = defaultGeometryFactory.toGeometry(geom.getEnvelopeInternal)

    // conduct the search through the various candidate resolutions
    val (_, ghOpt) = resolutions.range.foldRight((resolutions.minBitsResolution, Option.empty[GeoHash])){
      case (bits, orig@(res, _)) =>
        val gh = GeoHash(centroid.getX, centroid.getY, bits)
        if (gh.contains(env) && bits >= res) (bits, Some(gh)) else orig
    }

    // validate that you found a usable result
    val gh = ghOpt.getOrElse(GeoHash(centroid.getX, centroid.getY, resolutions.minBitsResolution))
    if (!gh.contains(env) && !gh.geom.equals(env))
      throw new Exception("ERROR:  Could not find a suitable " +
        resolutions.minBitsResolution + "-bit MBR for the target geometry:  " +
        geom)

    gh
  }

  /**
   * Computes the centroid of the given geometry as a WGS84 point.
   *
   * @param geom the target geometry whose centroid is sought
   * @return the centroid of the given geometry
   */
  private def getCentroid(geom:Geometry) : Point = {
    val pt = geom.safeCentroid()
    geom.getFactory.createPoint(new Coordinate(pt.getX, pt.getY))
  }

  // represents a degenerate (empty) geometry
  private lazy val emptyGeometry: Geometry = WKTUtils.read("POLYGON((0 0,0 0,0 0,0 0,0 0))")

  /**
   * Utility class for the geometry-decomposition routines.  This represents
   * one GeoHash-cell candidate within a decomposition.
   *
   * @param gh the associated GeoHash cell
   * @param targetGeom the geometry being decomposed
   */
  abstract class DecompositionCandidate(val gh:GeoHash, val targetGeom:Geometry) {

    lazy val area: Double = Try(gh.getArea).getOrElse(0.0)
    val areaOutside: Double
    lazy val resolution: Int = gh.prec
    lazy val intersectsTarget: Boolean = Try(gh.intersects(targetGeom)).getOrElse(false)
    lazy val intersection: Geometry = Try(gh.intersection(targetGeom)).getOrElse(emptyGeometry)
    def isLT(than:DecompositionCandidate): Boolean = {
      if (areaOutside > than.areaOutside) true
      else {
        if (areaOutside == than.areaOutside) area < than.area
        else false
      }
    }
  }

  private class PointDecompositionCandidate(gh:GeoHash, targetGeom:Point)
    extends DecompositionCandidate(gh, targetGeom) {

    /**
     * If the GeoHash does not contain the point, then the entire cell's area is
     * outside of the target.  If the GeoHash does contain the point, then be
     * careful:  Only some fraction of the cell's area should count as overage
     * (otherwise, we can't favor smaller GeoHash cells in the decomposer).
     */
    override lazy val areaOutside: Double = area * (if (intersectsTarget) 0.75 else 1.0)
  }

  private class LineDecompositionCandidate(gh:GeoHash, targetGeom:MultiLineString, targetArea:Double)
    extends DecompositionCandidate(gh, targetGeom) {

    /**
     * If the GeoHash intersects the target lines, then the overlap is the
     * area of the GeoHash cell less the length of the intersection.  Otherwise,
     * they are disjoint, and the overlap is the entire area of the GeoHash cell.
     *
     * Yes, this mixes units, but it observes two trends:
     * 1.  the longer a segment intersects, the smaller the area outside will be;
     * 2.  the smaller a GeoHash cell, the smaller the area outside will be
     */
    override lazy val areaOutside : Double =
      if (intersectsTarget) area * (1.0 - intersection.getLength / targetArea)
      else area
  }

  private class PolygonDecompositionCandidate(gh:GeoHash, targetGeom:MultiPolygon)
    extends DecompositionCandidate(gh, targetGeom) {

    /**
     * If the GeoHash intersects the target polygon, then the overlap is the
     * area of the GeoHash cell less the area of the intersection.  Otherwise,
     * they are disjoint, and the overlap is the entire area of the GeoHash cell.
     */
    override lazy val areaOutside : Double =
      if (intersectsTarget) area - intersection.getArea
      else area
  }

  private def decompositionCandidateSorter(a:DecompositionCandidate, b:DecompositionCandidate): Boolean = a.isLT(b)

  /**
   * Decomposes the given polygon into a collection of disjoint GeoHash cells
   * so that these constraints are satisfied:
   * 1.  the total number of decomposed GeoHashes does not exceed the maximum
   *     specified as an argument
   * 2.  the resolution of the GeoHashes falls within the given range
   * 3.  when replacing larger boxes with smaller boxes, always decompose first
   *     the box that contains the most area outside the target polygon
   *
   * @param targetGeom the polygon to be decomposed
   * @param maxSize the maximum number of GeoHash cells into which this polygon
   *                should be decomposed
   * @param resolutions the list of acceptable resolutions for the GeoHash cells
   * @return the list of GeoHash cells into which this polygon was decomposed
   *         under the given constraints
   */
  private def decomposeGeometry_(targetGeom: Geometry, maxSize: Int, resolutions: ResolutionRange): List[GeoHash] = {
    lazy val geomCatcher = catching(classOf[Exception])
    lazy val targetLength : Double = geomCatcher.opt { targetGeom.getLength }.getOrElse(0.0)

    // qua factory
    def createDecompositionCandidate(gh: GeoHash): DecompositionCandidate = {
      // simple switch based on the geometry type
      targetGeom match {
        case multipoly: MultiPolygon    =>
          new PolygonDecompositionCandidate(gh, multipoly)
        case polygon: Polygon           =>
          new PolygonDecompositionCandidate(
            gh, new MultiPolygon(Array(polygon), polygon.getFactory))
        case line: LineString           =>
          new LineDecompositionCandidate(  // promote to a multi-line string of one element
            gh, new MultiLineString(Array(line), line.getFactory), targetLength)
        case multiLine: MultiLineString =>
          new LineDecompositionCandidate(gh, multiLine, targetLength)
        case point: Point               =>
          new PointDecompositionCandidate(gh, point)  // should never be called, but it works
        case _                          =>
          throw new Exception(s"Unsupported Geometry type for decomposition:  ${targetGeom.getClass.getName}")
      }
    }

    // recursive routine that will do the actual decomposition
    @tailrec
    def decomposeStep(candidates: List[DecompositionCandidate]): List[DecompositionCandidate] = {
      // complain, if needed
      if (candidates.size > maxSize) throw new Exception("Too many candidates upon entry.")
      else {
        // identify the partial to replace...
        // which of the single candidates contains the least overlap (area outside the target geometry)?
        // assume that these are always sorted so that they are in descending order of overlap-area
        val candidate : DecompositionCandidate = candidates.head
        val childResolution : Int = candidate.gh.prec+resolutions.numBitsIncrement

        // decompose this (worst) candidate into its four children
        val candidateBitSet : BitSet = candidate.gh.bitset
        val children:List[DecompositionCandidate] = resolutions.getNextChildren(candidateBitSet, candidate.gh.prec).map(childBitSet => {
          createDecompositionCandidate(GeoHash(childBitSet, childResolution))
        }).filter(child => child.intersectsTarget)

        // build the next iteration of candidates
        val newCandidates : List[DecompositionCandidate] = (candidates.tail ++ children).sortWith(decompositionCandidateSorter)

        // recurse, if appropriate
        if ((newCandidates.size <= maxSize) && (childResolution <= resolutions.maxBitsResolution)) {
          decomposeStep(newCandidates)
        } else candidates
      }
    }

    // identify the smallest GeoHash that contains the entire polygon
    val ghMBR = getMinimumBoundingGeohash(targetGeom, resolutions)
    val candidateMBR = createDecompositionCandidate(ghMBR)

    // recursively decompose worst choices
    val (keepers:List[DecompositionCandidate]) = decomposeStep(List(candidateMBR))

    // return only the keepers
    (for (keeper:DecompositionCandidate <- keepers) yield keeper.gh).toList
  }

  private def getDecomposableGeometry(targetGeom: Geometry): Geometry = targetGeom match {
    case _: Point                                            => targetGeom
    case _: Polygon                                          => targetGeom
    case _: LineString      if targetGeom.getNumPoints < 100 => targetGeom
    case _: MultiLineString if targetGeom.getNumPoints < 100 => targetGeom
    case _                                                   => targetGeom.convexHull
  }

  /**
   * Transforms a geometry with lon in (-inf, inf) and lat in [-90,90] to a geometry in whole earth BBOX
   * 1) any coords of geometry outside lon [-180,180] are transformed to be within [-180,180]
   *    (to avoid spatial4j validation errors)
   * 2) use spatial4j to create a geometry with inferred International Date Line crossings
   *    (if successive coordinates longitudinal difference is greater than 180)
   * Parts of geometries with lat outside [-90,90] are ignored.
   * To represent a geometry with successive coordinates having lon diff > 180 and not wrapping
   * the IDL, you must insert a waypoint such that the difference is less than 180
   */
  def getInternationalDateLineSafeGeometry(targetGeom: Geometry): Try[Geometry] = {

    def degreesLonTranslation(lon: Double): Double = (((lon + 180) / 360.0).floor * -360).toInt

    def translateCoord(coord: Coordinate): Coordinate = {
      new Coordinate(coord.x + degreesLonTranslation(coord.x), coord.y)
    }

    def translatePolygon(geometry: Geometry): Geometry =
      defaultGeometryFactory.createPolygon(geometry.getCoordinates.map(c => translateCoord(c)))

    def translateLineString(geometry: Geometry): Geometry =
      defaultGeometryFactory.createLineString(geometry.getCoordinates.map(c => translateCoord(c)))

    def translateMultiLineString(geometry: Geometry): Geometry = {
      val coords = Seq.tabulate(geometry.getNumGeometries)(geometry.getGeometryN)
      val translated = coords.map { c => translateLineString(c).asInstanceOf[LineString] }
      defaultGeometryFactory.createMultiLineString(translated.toArray)
    }

    def translateMultiPolygon(geometry: Geometry): Geometry = {
      val coords = Seq.tabulate(geometry.getNumGeometries)(geometry.getGeometryN)
      val translated = coords.map { c => translatePolygon(c).asInstanceOf[Polygon] }
      defaultGeometryFactory.createMultiPolygon(translated.toArray)
    }

    def translateMultiPoint(geometry: Geometry): Geometry =
      defaultGeometryFactory.createMultiPointFromCoords(geometry.getCoordinates.map(c => translateCoord(c)))

    def translatePoint(geometry: Geometry): Geometry = {
      defaultGeometryFactory.createPoint(translateCoord(geometry.getCoordinate))
    }

    def translateGeometry(geometry: Geometry): Geometry = {
      geometry match {
        case _: Polygon =>          translatePolygon(geometry)
        case _: LineString =>       translateLineString(geometry)
        case _: MultiLineString =>  translateMultiLineString(geometry)
        case _: MultiPolygon =>     translateMultiPolygon(geometry)
        case _: MultiPoint =>       translateMultiPoint(geometry)
        case _: Point =>            translatePoint(geometry)
      }
    }

    Try {
      // copy the geometry so that we don't modify the input - JTS mutates the geometry
      // don't use the defaultGeometryFactory as it has limited precision
      val copy = GeometryUtils.geoFactory.createGeometry(targetGeom)
      val withinBoundsGeom =
        if (targetGeom.getEnvelopeInternal.getMinX < -180 || targetGeom.getEnvelopeInternal.getMaxX > 180)
          translateGeometry(copy)
        else
          copy

      JtsSpatialContext.GEO.getShapeFactory.makeShape(withinBoundsGeom, true, true).getGeom
    }
  }

  /**
   * Quick-and-dirty sieve that ensures that we don't waste time decomposing
   * single points.
   */
  def decomposeGeometry(targetGeom: Geometry,
                        maxSize: Int = 100,
                        resolutions: ResolutionRange = ResolutionRange(0, 40, 5)): List[(Double, Double, Double, Double)] = {
  // quick hit to avoid wasting time for single points
    targetGeom match {
      case point: Point =>
        val gh = GeoHash(point.getX, point.getY, resolutions.maxBitsResolution)
        List((gh.bbox.ll.getX, gh.bbox.ll.getY, gh.bbox.ur.getX, gh.bbox.ur.getY))
      case gc: GeometryCollection =>
        List.tabulate(gc.getNumGeometries)(gc.getGeometryN).flatMap(decomposeGeometry(_, maxSize, resolutions)).distinct
      case _ =>
        val safeGeom = getInternationalDateLineSafeGeometry(targetGeom).getOrElse(targetGeom)
        val relaxed = getDecomposableGeometry(safeGeom)
        decomposeGeometry_(relaxed, maxSize, resolutions)
          .map(gh => (gh.bbox.ll.getX, gh.bbox.ll.getY, gh.bbox.ur.getX, gh.bbox.ur.getY))
    }
  }
}
