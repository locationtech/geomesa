/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.utils.geohash

import geomesa.utils.CartesianProductIterable

import collection.BitSet
import collection.immutable.Range.Inclusive
import collection.mutable.{HashSet => MutableHashSet}
import com.spatial4j.core.context.jts.JtsSpatialContext
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import geomesa.utils.text.WKTUtils
import scala.collection.immutable.HashSet
import scala.util.control.Exception.catching

/**
 * The following bits of code are related to common operations involving
 * GeoHashes, such as recommending a GeoHash precision for an enclosing
 * polygon; decomposing a polygon into a fixed number of subordinate
 * GeoHashes; enumerating possible sub-strings within subordinate GeoHashes
 * for a given polygon; etc.
 */
object GeohashUtils
  extends GeomDistance
  with Logging {

  // make sure the implicits related to distance are in-scope
  import Distance._

  // the list of allowable GeoHash characters
  val base32seq = GeoHash.base32.toSeq

  lazy val wholeEarthBBox = defaultGeometryFactory.createPolygon(Array[Coordinate] (
    new Coordinate(-180, 90),
    new Coordinate(-180, -90),
    new Coordinate(180, -90),
    new Coordinate(180, 90),
    new Coordinate(-180, 90)))

  // these three constants are used in identifying unique GeoHash sub-strings
  val Base32Padding = (0 to 7).map(i => List.fill(i)(base32seq))
  val BinaryPadding = (0 to 4).map(i => List.fill(i)(Seq('0', '1')))

  /**
   * Simple place-holder for a pair of resolutions, minimum and maximum, along
   * with an increment.
   *
   * @param minBitsResolution minimum number of bits (GeoHash) resolution to consider; must be ODD
   * @param maxBitsResolution maximum number of bits (GeoHash) resolution to consider; must be ODD
   */
  case class ResolutionRange(minBitsResolution:Int=5,
                             maxBitsResolution:Int=63,
                             numBitsIncrement:Int=2)
    extends Inclusive(minBitsResolution, maxBitsResolution, numBitsIncrement) {
    // validate resolution arguments
    if (minBitsResolution >= maxBitsResolution)
      throw new IllegalArgumentException("Minimum resolution must be strictly greater than maximum resolution.")

    override def toString(): String = "{" + minBitsResolution.toString + ", +" + numBitsIncrement + "..., " + maxBitsResolution.toString + "}"

    def getNumChildren: Int = 1 << numBitsIncrement

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

  /**
   * Simple method to convert WKT to a Geometry.
   *
   * @param wkt the well-known text to interpret
   * @return the resulting geometry
   */
  implicit def wkt2geom(wkt:String) : Geometry = WKTUtils.read(wkt)

  /**
   * Brute-force way to convert a GeoHash to WKT, typically on the way to reading
   * convert a GeoHash to WKT, sometimes used for output (for visualization in
   * Quantum GIS, for example).
   *
   * @param gh the GeoHash -- rectangle -- to convert
   * @return the WKT representation of this GeoHash (cell)
   */
  def getGeohashWKT(gh:GeoHash): String = WKTUtils.write(gh)

  // default precision model
  val maxRealisticGeoHashPrecision : Int = 45
  val numDistinctGridPoints: Long = 1L << ((maxRealisticGeoHashPrecision+1)/2).toLong
  val defaultPrecisionModel = new PrecisionModel(numDistinctGridPoints.toDouble)

  // default factory for WGS84
  val defaultGeometryFactory : GeometryFactory = new GeometryFactory(defaultPrecisionModel, 4326)

  def getGeohashPoints(gh:GeoHash) : (Point, Point, Point, Point, Point, Point) = {
    // the bounding box is the basis for all of these points
    val bbox = gh.bbox

    // compute the corners
    val lr : Point = bbox.lr
    val ul : Point = bbox.ul
    val ll : Point = bbox.ll
    val ur : Point = bbox.ur

    // compute the endpoints of the horizontal line that passes through the center
    val midLatitude = 0.5 * (lr.getY+ ul.getY)
    val cl = lr.getFactory.createPoint(new Coordinate(ul.getX, midLatitude))
    val cr = lr.getFactory.createPoint(new Coordinate(lr.getX, midLatitude))

    (ll, cl, ul, ur, cr, lr)
  }

  /**
   * The default unit for area of a WGS84 projection is square-degrees.  We
   * would typically rather operate on square-meters, so this is a simple
   * utility function to estimate the area of a GeoHash (rectangle) in square
   * meters.  This is only an estimate, as the GeoHash is not really a proper
   * rectangle when it is projected on to the Earth's surface.  Furthermore,
   * if you are using a single GeoHash to represent a collection (that spans
   * multiple latitudes), then you are compounding the error.
   *
   * @param gh the GeoHash whose area in square-meters is requested
   * @return the estimate of the square-meters area of this GeoHash
   */
  def getGeohashAreaSquareMeters(gh:GeoHash) : Double = {
    // extract key points from this GeoHash
    val (ll, cl, ul, _, cr, _) = getGeohashPoints(gh)

    val dx : Double = VincentyModel.getDistanceBetweenTwoPoints(cl, cr)  // measured at the center-latitude of the GeoHash
    val dy : Double = VincentyModel.getDistanceBetweenTwoPoints(ll, ul)  // constant at any longitude within a single GeoHash

    dx * dy
  }

  /**
   * Computes an extreme horizontal or veritical distance possible within
   * the given GeoHash.
   *
   * @param gh the target GeoHash
   * @return the extreme vertical or horizontal distance, measure in meters
   */

  def getGeohashExtremeDimensionMeters(gh:GeoHash, fExtreme:(Double,Double)=>Double) : Double = {
    // extract key points from this GeoHash
    val (ll, cl, ul, ur, cr, lr) = getGeohashPoints(gh)

    // overall extreme
    fExtreme(
      // horizontal distances
      fExtreme(
        VincentyModel.getDistanceBetweenTwoPoints(ll, lr),
        VincentyModel.getDistanceBetweenTwoPoints(ul, ur)
      ),
      // vertical distances
      fExtreme(
        VincentyModel.getDistanceBetweenTwoPoints(ll, ul),
        VincentyModel.getDistanceBetweenTwoPoints(lr, ur)
      )
    )
  }

  /**
   * Computes the greatest horizontal or veritical distance possible within
   * the given GeoHash.
   *
   * @param gh the target GeoHash
   * @return the greatest vertical or horizontal distance, measure in meters
   */

  def getGeohashMaxDimensionMeters(gh:GeoHash) : Double = getGeohashExtremeDimensionMeters(gh, scala.math.max)

  /**
   * Computes the least horizontal or veritical distance possible within
   * the given GeoHash.
   *
   * @param gh the target GeoHash
   * @return the least vertical or horizontal distance, measure in meters
   */

  def getGeohashMinDimensionMeters(gh:GeoHash) : Double = getGeohashExtremeDimensionMeters(gh, scala.math.min)

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
  def getMinimumBoundingGeohash(geom:Geometry, resolutions:ResolutionRange) : GeoHash = {
    // save yourself some effort by computing the geometry's centroid and envelope up front
    val centroid = getCentroid(geom)
    val env = defaultGeometryFactory.toGeometry(geom.getEnvelopeInternal)

    // conduct the search through the various candidate resolutions
    val (_, ghOpt) = resolutions.foldRight((resolutions.minBitsResolution, Option.empty[GeoHash])){
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
  def getCentroid(geom:Geometry) : Point = {
    val pt = geom.getCentroid
    geom.getFactory.createPoint(new Coordinate(pt.getX, pt.getY))
  }

  /**
   * Conataints the constraints that are used to guide the search for a bits-
   * resolution on a given geometry.
   *
   * It is unlikely that these defaults will need to change for most of our
   * processing.
   *
   * @param minSpanMeters the (estimated) minimum span on any GeoHash within this division
   * @param maxSpanMeters the (estimated) maximum span on any GeoHash within this division
   * @param minSmallestBoxes the fewest GeoHashes into which the geometry should be divisible
   * @param maxSmallestBoxes the most GeoHashes into which the geometry should be divisible
   */
  class SizingConstraints(val minSpanMeters: Double=10 meters,
                          val maxSpanMeters:Double=10 kilometers,
                          val minSmallestBoxes:Long=2e4.asInstanceOf[Long],
                          val maxSmallestBoxes:Long=3e5.asInstanceOf[Long]) {
    // validate arguments
    if (minSpanMeters >= maxSpanMeters)
      throw new IllegalArgumentException("Minimum span must be strictly greater than maximum span.")
    if (minSmallestBoxes >= maxSmallestBoxes)
      throw new IllegalArgumentException("The lower-bound number of GeoHashes must be strictly greater than the upper bound.")

    def isSatisfiedBy(numBoxes:Long, getArea:()=>Double, getSpans:()=>(Double,Double)) : Boolean = {
      if (numBoxes >= minSmallestBoxes && numBoxes <= maxSmallestBoxes) {
        val (minSpan, maxSpan) = getSpans()
        minSpan >= minSpanMeters && maxSpan <= maxSpanMeters
      } else {
        false
      }
    }

    override def toString : String = "(" +
      "boxes " + minSmallestBoxes + " to " + maxSmallestBoxes + ", " +
      "spans in meters " + minSpanMeters + " to " + maxSpanMeters + ", " +
      ")"
  }

  /**
   * Miscellaneous utilities for dealing with geometry-sizing, such as determining
   * a reasonable number of bits (GeoHash) resolution for sizing polygons.
   */
  object GeometrySizingUtilities {
    /**
     * Container for the two values that together constitute a recommended number
     * of bits resolution to use for GeoHashes for a polygon.
     *
     * @param bits the resolution, in bits, for the target polygon
     * @param expectedNumGeohashes how many GeoHashes we expect the target
     *                             polygon to be decomposed into at the recommended
     *                             resolution
     */
    case class RecommendedResolution(bits: Int, expectedNumGeohashes: Long)

    /**
     * Given a geometry and constraints, return a recommendation for the number of
     * bits to use for expressing this polygon as a collection of GeoHashes.
     *
     * The approach is simple:  Given the MBR, compute the ratio of the area of
     * the target geometry to its bounding rectangle.  As the resolution of the
     * GeoHashes increases, the ratio of the number that will be inside the target
     * geometry to the total number inside the MBR should approach the ratio of
     * the areas.  This principle guides the estimate, and should be reasonable
     * for any sufficiently (non-trivially) large value of bits-resolution.
     *
     * @param geom the target geometry
     * @param resolutions the range of resolutions over which to range
     * @param constraints defining the acceptance criteria for this search
     *@return the pair of (recommended resolution, expected number of GeoHashes)
     *         that best satisfy the given constraints
     */
    def getRecommendedBitsResolutionForPolygon(geom:Geometry, resolutions:ResolutionRange=new ResolutionRange(), constraints:SizingConstraints=new SizingConstraints()) : RecommendedResolution = {
      lazy val geomCatcher = catching(classOf[Exception])

      // compute the MBR enclosing the target geometry
      val ghMBR = getMinimumBoundingGeohash(geom, resolutions)

      // compute the ratio of the area of the target geometry to its MBR
      val areaMBR = ghMBR.getArea
      val areaGeom = geomCatcher.opt { geom.getArea }.getOrElse(0.0)
      val pMBRGeom = areaGeom / areaMBR

      // pre-compute the centroid, because this will guide the satisfaction of the
      // per-GeoHash area constraint
      val centroid = getCentroid(geom)

      // consider candidate resolutions
      val (maxBits, numBoxes) = resolutions.foldLeft(resolutions.minBitsResolution, 0L){(res, bits) =>
      // at this resolution -- odds only -- how many boxes as our original MBR become?
        val newBits = bits - ghMBR.prec
        val numBoxes : Long = 1L << newBits.asInstanceOf[Long]

        // we expect only (geometryArea / MBRArea) of these total boxes to intersect the target geometry
        val expectedBoxes : Long = scala.math.ceil(numBoxes * pMBRGeom).asInstanceOf[Long]

        // define a function that can return the area (in square meters) of the centroid
        // for this geometry; it's a function rather than a value for lazy evaluation
        val fArea : () => Double = () => getGeohashAreaSquareMeters(GeoHash(centroid.getX, centroid.getY, bits))
        val fSpans : () => (Double, Double) = () => {
          val centroidGH = GeoHash(centroid.getX, centroid.getY, bits)
          (getGeohashMinDimensionMeters(centroidGH), getGeohashMaxDimensionMeters(centroidGH))
        }

        if (constraints.isSatisfiedBy(expectedBoxes, fArea, fSpans)) (bits, expectedBoxes) else res
      }

      if (numBoxes==0)
        throw new Exception("Could not satisfy constraints, resolutions " + resolutions + ", constraints " + constraints + ".")

      RecommendedResolution(maxBits, numBoxes)
    }
  }

  // represents a degenerate (empty) geometry
  lazy val emptyGeometry = WKTUtils.read("POLYGON((0 0,0 0,0 0,0 0,0 0))")

  /**
   * Utility class for the geometry-decomposition routines.  This represents
   * one GeoHash-cell candidate within a decomposition.
   *
   * @param gh the associated GeoHash cell
   * @param targetGeom the geometry being decomposed
   */
  abstract class DecompositionCandidate(val gh:GeoHash,
                                        val targetGeom:Geometry,
                                        val targetArea:Double,
                                        val resolutions:ResolutionRange) {

    lazy val geomCatcher = catching(classOf[Exception])
    lazy val area: Double = geomCatcher.opt{ gh.getArea }.getOrElse(0.0)
    val areaOutside: Double
    lazy val areaInside: Double = area - areaOutside
    lazy val resolution: Int = gh.prec
    lazy val isResolutionOk: Boolean =
      resolution >= resolutions.minBitsResolution && resolution <= resolutions.maxBitsResolution

    lazy val intersectsTarget: Boolean =
      geomCatcher.opt { gh.intersects(targetGeom) }.getOrElse(false)
    lazy val intersection: Geometry =
      geomCatcher.opt { gh.intersection(targetGeom) }.getOrElse(emptyGeometry)
    lazy val intersectionArea: Double =
      geomCatcher.opt { gh.intersection(targetGeom).getArea }.getOrElse(0.0)
    def isLT(than:DecompositionCandidate): Boolean = {
      if (areaOutside > than.areaOutside) true
      else {
        if (areaOutside == than.areaOutside) area < than.area
        else false
      }
    }
  }

  class PointDecompositionCandidate(gh:GeoHash,
                                    targetGeom:Point,
                                    targetArea:Double,
                                    resolutions:ResolutionRange)
    extends DecompositionCandidate(gh, targetGeom, targetArea, resolutions) {

    /**
     * If the GeoHash does not contain the point, then the entire cell's area is
     * outside of the target.  If the GeoHash does contain the point, then be
     * careful:  Only some fraction of the cell's area should count as overage
     * (otherwise, we can't favor smaller GeoHash cells in the decomposer).
     */
    override lazy val areaOutside: Double = area * (if (intersectsTarget) 0.75 else 1.0)
  }

  class LineDecompositionCandidate(gh:GeoHash,
                                   targetGeom:MultiLineString,
                                   targetArea:Double,
                                   resolutions:ResolutionRange)
    extends DecompositionCandidate(gh, targetGeom, targetArea, resolutions) {

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

  class PolygonDecompositionCandidate(gh:GeoHash,
                                      targetGeom:MultiPolygon,
                                      targetArea:Double,
                                      resolutions:ResolutionRange)
    extends DecompositionCandidate(gh, targetGeom, targetArea, resolutions) {

    /**
     * If the GeoHash intersects the target polygon, then the overlap is the
     * area of the GeoHash cell less the area of the intersection.  Otherwise,
     * they are disjoint, and the overlap is the entire area of the GeoHash cell.
     */
    override lazy val areaOutside : Double =
      if (intersectsTarget) area - intersection.getArea
      else area
  }

  def decompositionCandidateSorter(a:DecompositionCandidate,
                                   b:DecompositionCandidate): Boolean = a.isLT(b)

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
  private def decomposeGeometry_(targetGeom: Geometry,
                                 maxSize: Int = 100,
                                 resolutions: ResolutionRange = new ResolutionRange(5,40,5)): List[GeoHash] = {
    lazy val geomCatcher = catching(classOf[Exception])
    val targetArea : Double = geomCatcher.opt { targetGeom.getArea }.getOrElse(0.0)
    val targetLength : Double = geomCatcher.opt { targetGeom.getLength }.getOrElse(0.0)

    // qua factory
    def createDecompositionCandidate(gh: GeoHash): DecompositionCandidate = {
      // simple switch based on the geometry type
      targetGeom match {
        case multipoly: MultiPolygon    =>
          new PolygonDecompositionCandidate(gh, multipoly, targetArea, resolutions)
        case polygon: Polygon           =>
          new PolygonDecompositionCandidate(
            gh, new MultiPolygon(Array(polygon), polygon.getFactory), targetArea, resolutions)
        case line: LineString           =>
          new LineDecompositionCandidate(  // promote to a multi-line string of one element
            gh, new MultiLineString(Array(line), line.getFactory), targetLength, resolutions)
        case multiLine: MultiLineString =>
          new LineDecompositionCandidate(gh, multiLine, targetLength, resolutions)
        case point: Point               =>
          new PointDecompositionCandidate(gh, point, targetArea, resolutions)  // should never be called, but it works
        case _                          =>
          throw new Exception(s"Unsupported Geometry type for decomposition:  ${targetGeom.getClass.getName}")
      }
    }

    // recursive routine that will do the actual decomposition
    def decomposeStep(candidates: List[DecompositionCandidate]): List[DecompositionCandidate] = {
      // complain, if needed
      if (candidates.size > maxSize) throw new Exception("Too many candidates upon entry.")
      else {
        // identify the partial to replace...
        // which of the single candidates contains the least overlap (area outside the target geometry)?
        // assume that these are always sorted so that they are in descending order of overlap-area
        val candidate : DecompositionCandidate = candidates(0)
        val childResolution : Int = candidate.gh.prec+resolutions.numBitsIncrement

        // decompose this (worst) candidate into its four children
        val candidateBitSet : BitSet = candidate.gh.bitset
        val children:List[DecompositionCandidate] = resolutions.getNextChildren(candidateBitSet, candidate.gh.prec).map((childBitSet) => {
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

  def getDecomposableGeometry(targetGeom: Geometry): Geometry = targetGeom match {
    case g: Point                                            => targetGeom
    case g: Polygon                                          => targetGeom
    case g: LineString      if targetGeom.getNumPoints < 100 => targetGeom
    case g: MultiLineString if targetGeom.getNumPoints < 100 => targetGeom
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
  def getInternationalDateLineSafeGeometry(targetGeom: Geometry): Geometry = {

    def degreesLonTranslation(lon: Double): Double = (((lon + 180) / 360.0).floor * -360).toInt

    def translateCoord(coord: Coordinate): Coordinate = {
      new Coordinate(coord.x + degreesLonTranslation(coord.x), coord.y)
    }

    def translatePolygon(geometry: Geometry): Geometry =
      defaultGeometryFactory.createPolygon(geometry.getCoordinates.map(c => translateCoord(c)))

    def translateLineString(geometry: Geometry): Geometry =
      defaultGeometryFactory.createLineString(geometry.getCoordinates.map(c => translateCoord(c)))

    def translateMultiLineString(geometry: Geometry): Geometry = {
      val coords = (0 until geometry.getNumGeometries).map { i => geometry.getGeometryN(i) }
      val translated = coords.map { c => translateLineString(c).asInstanceOf[LineString] }
      defaultGeometryFactory.createMultiLineString(translated.toArray)
    }

    def translateMultiPolygon(geometry: Geometry): Geometry = {
      val coords = (0 until geometry.getNumGeometries).map { i => geometry.getGeometryN(i) }
      val translated = coords.map { c => translatePolygon(c).asInstanceOf[Polygon] }
      defaultGeometryFactory.createMultiPolygon(translated.toArray)
    }

    def translateMultiPoint(geometry: Geometry): Geometry =
      defaultGeometryFactory.createMultiPoint(geometry.getCoordinates.map(c => translateCoord(c)))

    def translatePoint(geometry: Geometry): Geometry = {
      defaultGeometryFactory.createPoint(translateCoord(geometry.getCoordinate))
    }

    def translateGeometry(geometry: Geometry): Geometry = {
      geometry match {
        case p: Polygon =>          translatePolygon(geometry)
        case l: LineString =>       translateLineString(geometry)
        case m: MultiLineString =>  translateMultiLineString(geometry)
        case m: MultiPolygon =>     translateMultiPolygon(geometry)
        case m: MultiPoint =>       translateMultiPoint(geometry)
        case p: Point =>            translatePoint(geometry)
      }
    }

    val withinBoundsGeom =
      if (targetGeom.getEnvelopeInternal.getMinX < -180 || targetGeom.getEnvelopeInternal.getMaxX > 180)
        translateGeometry(targetGeom)
      else
        targetGeom

    val shape = JtsSpatialContext.GEO.makeShape(withinBoundsGeom, true, true)
    shape.getGeom
  }

  /**
   * Quick-and-dirty sieve that ensures that we don't waste time decomposing
   * single points.
   */
  def decomposeGeometry(targetGeom: Geometry,
                        maxSize: Int = 100,
                        resolutions: ResolutionRange = new ResolutionRange(0, 40, 5),
                        relaxFit: Boolean = true): List[GeoHash] =
    // quick hit to avoid wasting time for single points
    targetGeom match {
      case point: Point => List(GeoHash(point.getX, point.getY, resolutions.maxBitsResolution))
      case _ =>
        val safeGeom = getInternationalDateLineSafeGeometry(targetGeom)
        decomposeGeometry_(
          if (relaxFit) getDecomposableGeometry(safeGeom)
          else safeGeom, maxSize, resolutions)
    }

  /**
   * Given a geometry, estimate how many bits precision would be required to
   * construct a GeoHash-rectangle that has roughly the same bounding-box.
   *
   * This method does not account for any specific latitude!
   */
  def estimateGeometryGeohashPrecision(geometry: Geometry): Int = {
    if (geometry == null) 0
    else {
      // compute the span (in degrees) of this geometry
      val envelope = geometry.getEnvelopeInternal
      val spanLongitude= envelope.getWidth
      val spanLatitude = envelope.getHeight

      // estimate the number of divisions in each direction
      val numLatitudeDivisions = math.log(180.0/spanLatitude) / math.log(2.0)
      val numLongitudeDivisions = math.log(360.0/spanLongitude) / math.log(2.0)

      // precision is the sum of these two
      math.round(numLatitudeDivisions + numLongitudeDivisions).toInt
    }
  }

  /**
   * Given a rectangular geometry, compute which GeoHash it most likely
   * represented.
   *
   * @param geometry the geometric expression of a GeoHash
   * @return the most likely GeoHash that is represented by the given geometry
   */
  def reconstructGeohashFromGeometry(geometry: Geometry): GeoHash = geometry match {
    case null => throw new Exception("Invalid geometry:  null")
    case _ if "Point".equals(geometry.getGeometryType) => GeoHash(geometry.asInstanceOf[Point], maxRealisticGeoHashPrecision)
    case _ if geometry.isRectangle => GeoHash(geometry.getCentroid, estimateGeometryGeohashPrecision(geometry))
    case m: MultiPolygon =>
      if(m.getNumGeometries != 1) throw new Exception("Expected simple geometry")
      else if(!m.getGeometryN(0).isRectangle) throw new Exception("Expected rectangular geometry")
      else GeoHash(m.getGeometryN(0).getCentroid, estimateGeometryGeohashPrecision(m.getGeometryN(0)))
    case _ => throw new Exception(s"Invalid geometry:  $geometry")
  }

  /**
   * Given a collection of GeoHash hash sub-strings, this routine
   * will build an iterator that generates all dotted variants.
   * "Dotted" in this context means supporting the abstracted
   * representation (in which higher-level hash-strings use periods
   * for the base-32 characters they don't use).  For example,
   * if the string is "dqb", then the dotted variants include all
   * of the following:
   *
   *   dqb
   *   dq.
   *   d..
   *   ...
   *
   * @param set the collection of GeoHash hash substrings to dot
   * @param maxSize the maximum allowable number of entries in the final
   *                iterator
   * @return an iterator over the dotted collection of hash substrings,
   *         constrained to one more than the maximum allowable size
   *         (to enable overflow-detection on the outside)
   */
  def getGeohashStringDottingIterator(set: Seq[String], maxSize: Int): Iterator[String] = {
    val len = set.headOption.map(_.length).getOrElse(0)
    (for {
      i <- (0 to len).iterator
      hash <- set.map(_.take(i)).distinct
      newStr = hash.take(i) + "".padTo(len - i, ".").mkString
    } yield newStr).take(maxSize + 1)
  }

  /**
   * Given an index-schema format such as "%1,3#gh", it becomes necessary to
   * identify which unique 3-character GeoHash sub-strings intersect the
   * query polygon.  This routine performs exactly such an identification.
   *
   * The full GeoHashes from which the sub-strings are extracted are computed
   * at 35 bits.
   *
   * Computing all of the 35-bit GeoHashes that intersect with the target
   * geometry can take too long.  Instead, we start with the minimum-bounding
   * GeoHash (which might be 0 bits), and recursively dividing it in two
   * while remembering those GeoHashes that are completely contained in the
   * target geometry.  This has a few advantages:
   *
   * 1.  we can stop recursing into GeoHashes at the coarsest
   *     level (largest geometry) possible when they stop intersecting
   *     the target geometry;
   * 2.  instead of enumerating all of the GeoHashes that intersect, we
   *     can stop as soon as we know that all possible children are known
   *     to be inside the target geometry; that is, if a 13-bit GeoHash
   *     is covered by the target, then we know that all 15-bit GeoHashes
  *     that are its children will also be covered by the target
   * 3.  if we ever find a GeoHash that is entirely covered by the target
   *     geometry whose precision is no more than 5 times the "offset"
   *     parameter's number of bits, then we can stop, because all possible
   *     combinations are known to be used
   *
   * As an example, consider trying to enumerate the (3, 2) sub-strings of
   * GeoHashes in a polygon that is only slightly inset within the
   * Southern hemisphere.  This implicates a large number of 25-bit
   * GeoHashes, but as soon as one of the GeoHashes that has 15 or fewer
   * bits is found that is covered by the target, the search can stop
   * for unique prefixes, because all of its 25-bit children will be
   * distinct and will also be covered by the target.
   *
   * This is easier to explain with pictures.
   *
   * @param poly the query-polygon that must intersect candidate GeoHashes
   * @param offset how many of the left-most GeoHash characters to skip
   * @param bits how many of the (remaining) GeoHash characters to use
   * @param MAX_KEYS_IN_LIST the maximum allowable number of unique GeoHash
   *                         sub-strings; when exceeded, the function returns
   *                         an empty list
   *
   * @return the list of unique GeoHash sub-strings from 35-bits precision that
   *         intersect the target polygon; an empty list if there are too many
   */
  def getUniqueGeohashSubstringsInPolygon(poly: Polygon,
                                          offset: Int,
                                          bits: Int,
                                          MAX_KEYS_IN_LIST: Int = Int.MaxValue,
                                          includeDots: Boolean = true): Seq[String] = {

    val maxBits = (offset + bits) * 5
    val minBits = offset * 5
    val usedBits = bits * 5
    val allResolutions = ResolutionRange(0, Math.min(35, maxBits), 1)
    val maxKeys = Math.min(2 << usedBits, MAX_KEYS_IN_LIST)
    val polyCentroid = poly.getCentroid

    // find the smallest GeoHash you can that covers the target geometry
    val ghMBR = getMinimumBoundingGeohash(poly, allResolutions)

    // this case-class closes over properties of the current search
    case class BitPrefixes(prefixes: Seq[String]) {

      val hasEverythingPrefix = prefixes.exists(prefix => prefix.length <= minBits)

      // how many GeoHashes are entailed by the list of prefixes
      val entailedSize =
        if (hasEverythingPrefix) maxKeys
        else Math.min(
            1 << usedBits,
            prefixes.foldLeft(0)((sumSoFar, prefix) => {
              sumSoFar + (1 << Math.min(usedBits, maxBits - prefix.length))
            }))

      // is there any prefix wholly contained within the target geometry
      // that uses fewer than 5*offset bits?  if so, then all possible
      // sub-strings are entailed
      val usesAll = prefixes.exists(prefix => prefix.length <= minBits) ||
        entailedSize == maxKeys

      // the loose inequality is so that we can detect overflow
      def hasRoom = entailedSize <= maxKeys

      def notDone = !usesAll && hasRoom

      def overflowed =
        if (usesAll) {
          (1 << usedBits) > maxKeys
        } else {
          entailedSize > maxKeys
        }

      // generate all combinations of GeoHash strings of
      // the desired length
      def generateAll(prefix: String): Seq[String] = {
        val prefixHash = GeoHash.fromBinaryString(prefix).hash
        if (prefixHash.length < bits) {
          val charSeqs = Base32Padding(bits - prefixHash.length)
          CartesianProductIterable(charSeqs).toList.map(prefixHash + _.mkString)
        } else Seq(prefixHash)
      }

      // generate all of the combinations entailed by the prefixes identified,
      // all of which will have bits that overlap with the requested substring
      // (that is, if we want (3,2), then all of the prefixes we identified have
      // somewhere between 15 and 25 bits in them)
      //
      // each prefix, then, needs to be expanded to a list of all combinations
      // of bits that reach the next 5-bit boundary, and then those prefixes
      // can be expanded to use all combinations of base-32 characters that
      // allow them to fill out the requisite range
      def generateSome: Seq[String] = {
        prefixes.foldLeft(HashSet[String]())((ghsSoFar, prefix) => {
          // fill out this prefix to the next 5-bit boundary
          val bitsToBoundary = (65 - prefix.length) % 5
          val bases =
            if (bitsToBoundary == 0) Seq(prefix)
            else {
              val fillers = BinaryPadding(bitsToBoundary)
              val result = CartesianProductIterable(fillers).toList.map(prefix + _.mkString)
              result
            }
          bases.foldLeft(ghsSoFar)((ghs, base) => {
            val baseTrimmed = base.drop(minBits)
            val newSubs = generateAll(baseTrimmed)
            ghs ++ newSubs
          })
        }).toSeq
      }

      def toSeq: Seq[String] =
        if (usesAll) generateAll("")
        else generateSome
    }

    // assume that this method is never called on a GeoHash
    // whose binary-string encoding is too long
    def considerCandidate(candidate: GeoHash): Seq[String] = {
      val bitString = candidate.toBinaryString

      if (!poly.intersects(candidate.geom)) return Nil

      if (poly.covers(candidate.geom) || (bitString.size == maxBits)) {
        Seq(bitString)
      } else {
        if (bitString.size < maxBits) {
          // choose which direction to recurse into next by proximity
          // of the two child GeoHashes to the polygon's centroid;
          // for rectangles or polygons whose area is concentrated
          // near the centroid, this provides for a a HUGE speed increase
          val gh0 = GeoHash.fromBinaryString(bitString + "0")
          val gh1 = GeoHash.fromBinaryString(bitString + "1")
          val d0 = Math.hypot(gh0.getPoint.getX - polyCentroid.getX, gh0.getPoint.getY - polyCentroid.getY)
          val d1 = Math.hypot(gh1.getPoint.getX - polyCentroid.getX, gh1.getPoint.getY - polyCentroid.getY)
          val (firstChild, secondChild) =
            if (d0 <= d1) (gh0, gh1)
            else (gh1, gh0)

          val firstChildList = considerCandidate(firstChild)

          // if you've found an entry that entails all sub-strings, stop searching
          firstChildList ++ (firstChildList.headOption match {
            case Some(bitStr) if bitStr.length <= minBits => Nil
            case _                                        =>
              considerCandidate(secondChild)
          })
        } else Nil
      }
    }

    // compute the list of acceptable prefixes
    val bitPrefixes = BitPrefixes(
      if (ghMBR.prec <= maxBits) considerCandidate(ghMBR)
      else Seq(ghMBR.toBinaryString.drop(minBits).take(usedBits)))

    // detect overflow
    if (bitPrefixes.overflowed) return Seq()

    // not having overflowed, turn the collection of disjoint prefixes
    // into a list of full geohash substrings
    val unDotted = bitPrefixes.toSeq

    // add dotted versions, if appropriate (to match decomposed GeoHashes that
    // may be encoded at less than a full 35-bits precision)
    if (includeDots) {
      if (unDotted.size < maxKeys) {
        // STOP as soon as you've exceeded the maximum allowable entries
        val keepers = getGeohashStringDottingIterator(
          unDotted, MAX_KEYS_IN_LIST).take(MAX_KEYS_IN_LIST + 1).toList
        if (keepers.size <= MAX_KEYS_IN_LIST) keepers.toSeq else Seq()
      } else Seq()
    } else unDotted
  }
}
