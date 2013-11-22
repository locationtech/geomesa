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

import com.vividsolutions.jts.geom._
import geomesa.utils.text.WKTUtils
import scala.collection.BitSet
import scala.util.control.Exception.catching

/**
 * The following bits of code are related to generating an automatic estimate
 * of the resolution that ought to be used for a particular polygon, such as
 * an area of operations (AO).
 *
 * These routines have been bundled into a trait, so that it can be mixed into
 * other a geographic/geometric module that can itself be imported into, for
 * example, the <code>AOService</code>.
 *
 * Long term, clearly, if we keep this kind of idea, the code should be merged
 * in to some other location.  "commons-geo" might be a good candidate.  For
 * now, the idea is to give this a try, and see whether it's worth preserving
 * at all.
 */
object GeohashUtils extends GeomDistance {
  // make sure the implicits related to distance are in-scope

  import Distance._

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
    extends Range(minBitsResolution, maxBitsResolution, numBitsIncrement) {
    // validate resolution arguments
    if (minBitsResolution >= maxBitsResolution)
      throw new IllegalArgumentException("Minimum resolution must be strictly greater than maximum resolution.")

    override def toString() : String = "{" + minBitsResolution.toString + ", +" + numBitsIncrement + "..., " + maxBitsResolution.toString + "}"

    def getNumChildren : Int = (1 << numBitsIncrement)

    def getNextChildren(parent:BitSet, oldPrecision:Int) : List[BitSet] = {
      (Range(0, getNumChildren).map((i:Int) => {
        // compute bit-set corresponding to this integer,
        // and add it to the left-shifted version of the parent
        val bitString : String = i.toBinaryString

        Range(0, numBitsIncrement).foldLeft(parent)((bs:BitSet,j:Int) => {
          val c : Char = if (j<bitString.length) bitString.charAt(j) else '0'

          if (c=='1') (bs + (oldPrecision + bitString.length - 1 - j))
          else bs
        })
      })).toList
    }
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
   * this as a geometry, but sometimes used for output (for visualization in
   * Quantum GIS, for example).
   *
   * @param gh the GeoHash -- rectangle -- to convert
   * @return the WKT representation of this GeoHash (cell)
   */
  def getGeohashWKT(gh:GeoHash) : String = {
    val bbox = gh.bbox
    val y0 = bbox.ll.getY
    val y1 = bbox.ur.getY
    val x0 = bbox.ll.getX
    val x1 = bbox.ur.getX

    "POLYGON((" + x0 + " " + y0 + "," + x0 + " " + y1 + "," + x1 + " " + y1 + "," + x1 + " " + y0 + "," + x0 + " " + y0 + "))"
  }

  // default precision model
  val maxRealisticGeoHashPrecision : Int = 45
  val numDistinctGridPoints : Long = 1L << ((maxRealisticGeoHashPrecision+1)/2).toLong
  val defaultPrecisionModel = new PrecisionModel(numDistinctGridPoints.toDouble)

  // default factory for WGS84
  val defaultGeometryFactory : GeometryFactory = new GeometryFactory(defaultPrecisionModel, 4326)

  /**
   * Converts a GeoHash to a geometry by way of WKT.
   *
   * @param gh the GeoHash -- rectangle -- to convert
   * @return the Geometry version of this GeoHash
   */
  def getGeohashGeom(gh:GeoHash) : Geometry = {
    val ring : LinearRing = defaultGeometryFactory.createLinearRing(
      Array(
        new Coordinate(gh.bbox.ll.getX, gh.bbox.ll.getY),
        new Coordinate(gh.bbox.ll.getX, gh.bbox.ur.getY),
        new Coordinate(gh.bbox.ur.getX, gh.bbox.ur.getY),
        new Coordinate(gh.bbox.ur.getX, gh.bbox.ll.getY),
        new Coordinate(gh.bbox.ll.getX, gh.bbox.ll.getY)
      )
    )
    defaultGeometryFactory.createPolygon(ring, null)
  }

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
    val (ll, cl, ul, ur, cr, lr) = getGeohashPoints(gh)

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
   * any other point).  Sorry, talking myself into believing that this computes
   * the MBR instead of just some lazy BR.
   *
   * @param geom the target geometry that is to be enclosed
   * @return the smallest GeoHash -- at an odd number of bits resolution -- that
   *         completely encloses the target geometry
   */
  def getMinimumBoundingGeohash(geom:Geometry, resolutions:ResolutionRange) : GeoHash = {
    // save yourself some effort by computing the geometry's centroid up front
    val centroid = getCentroid(geom)

    // conduct the search through the various candidate resolutions
    val (maxBits:Int, ghOpt:Option[GeoHash]) = resolutions.foldRight((resolutions.minBitsResolution, None:Option[GeoHash])){(bits:Int,res:(Int,Option[GeoHash])) =>
      val gh = GeoHash(centroid.getX, centroid.getY, bits)
      if (getGeohashGeom(gh).covers(geom) && bits>=res._1) (bits, Some(gh)) else res
    }

    // validate that you found a usable result
    val gh:GeoHash = ghOpt.getOrElse(GeoHash(centroid.getX, centroid.getY, resolutions.minBitsResolution))
    if (!getGeohashGeom(gh).contains(geom))
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
        val (minSpan:Double, maxSpan:Double) = getSpans()
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
      val areaMBR = getGeohashGeom(ghMBR).getArea
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
  abstract class DecompositionCandidate(val gh:GeoHash, val targetGeom:Geometry,
                                        val targetArea:Double, val resolutions:ResolutionRange) {

    lazy val geomCatcher = catching(classOf[Exception])
    lazy val geom : Geometry = getGeohashGeom(gh)
    lazy val area : Double = geomCatcher.opt{ geom.getArea }.getOrElse(0.0)
    val areaOutside : Double
    lazy val areaInside : Double = area - areaOutside
    lazy val resolution : Int = gh.prec
    lazy val isResolutionOk : Boolean =
      (resolution >= resolutions.minBitsResolution &&
        resolution <= resolutions.maxBitsResolution)
    lazy val intersectsTarget : Boolean =
      geomCatcher.opt { geom.intersects(targetGeom) }.getOrElse(false)
    lazy val intersection : Geometry =
      geomCatcher.opt { geom.intersection(targetGeom) }.getOrElse(emptyGeometry)
    lazy val intersectionArea : Double =
      geomCatcher.opt { geom.intersection(targetGeom).getArea }.getOrElse(0.0)
    def isLT(than:DecompositionCandidate) : Boolean = {
      if (areaOutside > than.areaOutside) true
      else {
        if (areaOutside == than.areaOutside) area < than.area
        else false
      }
    }
  }

  class PointDecompositionCandidate(gh:GeoHash, targetGeom:Point, targetArea:Double, resolutions:ResolutionRange) extends DecompositionCandidate(gh, targetGeom, targetArea, resolutions) {
    /**
     * If the GeoHash does not contain the point, then the entire cell's area is
     * outside of the target.  If the GeoHash does contain the point, then be
     * careful:  Only some fraction of the cell's area should count as overage
     * (otherwise, we can't favor smaller GeoHash cells in the decomposer).
     */
    override lazy val areaOutside : Double = area * (if (intersectsTarget) 0.75 else 1.0)
  }

  class LineDecompositionCandidate(gh:GeoHash, targetGeom:MultiLineString, targetArea:Double, resolutions:ResolutionRange) extends DecompositionCandidate(gh, targetGeom, targetArea, resolutions) {
    /**
     * If the GeoHash intersects the target lines, then the overlap is the
     * area of the GeoHash cell less the length of the intersection.  Otherwise,
     * they are disjoint, and the overlap is the entire area of the GeoHash cell.
     *
     * Yes, this mixes units, but it observes two trends:
     * 1.  the longer a segment intersects, the smaller the area outside will be;
     * 2.  the smaller a GeoHash cell, the smaller the area outside will be
     */
    override lazy val areaOutside : Double = {
      if (intersectsTarget) {
        area * (1.0 - intersection.getLength / targetArea)
      } else {
        area
      }
    }
  }

  class PolygonDecompositionCandidate(gh:GeoHash, targetGeom:MultiPolygon, targetArea:Double, resolutions:ResolutionRange) extends DecompositionCandidate(gh, targetGeom, targetArea, resolutions) {
    /**
     * If the GeoHash intersects the target polygon, then the overlap is the
     * area of the GeoHash cell less the area of the intersection.  Otherwise,
     * they are disjoint, and the overlap is the entire area of the GeoHash cell.
     */
    override lazy val areaOutside : Double = {
      if (intersectsTarget) {
        area - intersection.getArea
      } else {
        area
      }
    }
  }

  def decompositionCandidateSorter(a:DecompositionCandidate, b:DecompositionCandidate) : Boolean = a.isLT(b)

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
  private def decomposeGeometry_(targetGeom:Geometry, maxSize:Int=100, resolutions:ResolutionRange=new ResolutionRange(5,40,5)) : List[GeoHash] = {
    lazy val geomCatcher = catching(classOf[Exception])
    val targetArea : Double = geomCatcher.opt { targetGeom.getArea }.getOrElse(0.0)
    val targetLength : Double = geomCatcher.opt { targetGeom.getLength }.getOrElse(0.0)

    // qua factory
    def createDecompositionCandidate(gh:GeoHash) : DecompositionCandidate = {
      // simple switch based on the geometry type
      targetGeom match {
        case multipoly:MultiPolygon => new PolygonDecompositionCandidate(gh, multipoly, targetArea, resolutions)
        case polygon:Polygon => new PolygonDecompositionCandidate(
          gh, new MultiPolygon(Array(polygon), polygon.getFactory), targetArea, resolutions)
        case line:LineString => new LineDecompositionCandidate(  // promote to a multi-line string of one element
          gh, new MultiLineString(Array(line), line.getFactory), targetLength, resolutions)
        case multiLine:MultiLineString => new LineDecompositionCandidate(gh, multiLine, targetLength, resolutions)
        case point:Point => new PointDecompositionCandidate(gh, point, targetArea, resolutions)  // should never be called, but it works
        case _ => throw new Exception("Unsupported Geometry type for decomposition:  " + targetGeom.getClass.getName)
      }
    }

    // recursive routine that will do the actual decomposition
    def decomposeStep(candidates:List[DecompositionCandidate]) : (List[DecompositionCandidate]) = {
      // complain, if needed
      if (candidates.size >= maxSize) throw new Exception("Too many candidates upon entry.")
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
        if ((newCandidates.size < maxSize) && (childResolution <= resolutions.maxBitsResolution)) {
          decomposeStep(newCandidates)
        }
        else candidates
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

  /**
   * Quick-and-dirty sieve that ensures that we don't waste time decomposing
   * single points.
   */
  def decomposeGeometry(targetGeom:Geometry, maxSize:Int=100, resolutions:ResolutionRange=new ResolutionRange(0,40,5)) : List[GeoHash] = {
    // quick hit to avoid wasting time for single points
    targetGeom match {
      case point:Point => List(GeoHash(point.getX, point.getY, resolutions.maxBitsResolution))
      case _ => decomposeGeometry_(targetGeom, maxSize, resolutions)
    }
  }

  /**
   * Given a geometry, estimate how many bits precision would be required to
   * construct a GeoHash-rectangle that has roughly the same bounding-box.
   *
   * This method does not account for any specific latitude!
   */
  def estimateGeometryGeohashPrecision(geometry:Geometry) : Int = {
    if (geometry==null) 0
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
  def reconstructGeohashFromGeometry(geometry:Geometry) : GeoHash = {
    // you must have a rectangular geometry for this function to make any sense
    if (geometry==null) throw new Exception("Invalid geometry")
    if (!geometry.isRectangle) throw new Exception("Non-rectangular geometry")

    // the GeoHash always builds around the centroid, so compute that up front
    val centroid : Point = geometry.getCentroid

    // figure out the precision of this GeoHash
    val precision = estimateGeometryGeohashPrecision(geometry)

    GeoHash(centroid.getX, centroid.getY, precision.toInt)
  }

  /**
   * Given an index-schema format such as "%1,3#gh", it becomes necessary to
   * identify which unique 3-character GeoHash sub-strings intersect the
   * query polygon.  This routine performs exactly such an identification.
   *
   * The full GeoHashes from which the sub-strings are extracted are computed
   * at 35 bits.
   *
   * In the event that there are more such unique sub-strings than the maximum
   * allowable, return an empty list.
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
  def getUniqueGeohashSubstringsInPolygon(poly:Polygon,
                                          offset:Int,
                                          bits:Int,
                                          MAX_KEYS_IN_LIST:Int=Int.MaxValue):
  Seq[String] = {

    // the list of allowable GeoHash characters
    val base32seq = GeoHash.base32.toSeq

    // identify the MBR for the query polygon
    val env = poly.getEnvelopeInternal
    val bbox = BoundingBox(Bounds(env.getMinX, env.getMaxX), Bounds(env.getMinY,
      env.getMaxY))
    val covering = BoundingBox.getCoveringGeoHash(bbox, 35)

    val memoized = collection.mutable.HashSet.empty[String]

    def consider(gh: GeoHash, charsLeft: Int): Seq[GeoHash] =
      if (charsLeft > 0 && memoized.size < MAX_KEYS_IN_LIST) {
        for {
          newChar <- base32seq
          newGH = GeoHash(gh.hash + newChar) if memoized.size <= MAX_KEYS_IN_LIST && poly.intersects(newGH.bbox.geom)
          subHash = newGH.hash.drop(offset).take(bits)
          dummy = memoized.add(subHash)
          child <- consider(newGH, charsLeft - 1)
        } yield child
      } else {
        Seq(gh)
      }

    // how many characters total are left?
    val numCharsLeft = offset + bits - covering.hash.length
    val explicitHashes =
      consider(covering, numCharsLeft).map(_.hash.drop(offset).take(bits)).distinct

    // add dotted versions, if appropriate (to match decomposed GeoHashes that
    // may be encoded at less than a full 35-bits precision)
    val keepers = if (explicitHashes.size < MAX_KEYS_IN_LIST) {
      (for {
        hash <- explicitHashes
        i <- (0 to bits)
        newStr = hash.take(i) +  "".padTo(bits-i,".").mkString
      } yield newStr).distinct
    } else {
      Seq()
    }

    if (keepers.size <= MAX_KEYS_IN_LIST) keepers else Seq()
  }
}
