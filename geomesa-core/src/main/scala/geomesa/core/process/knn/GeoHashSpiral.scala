package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Point
import geomesa.utils.geohash._
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import geomesa.utils.geotools.GeometryUtils.distanceDegrees
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.collection.mutable

// used for ordering GeoHashes within a PriorityQueue
case class GeoHashWithDistance(gh: GeoHash, dist: Double)

/**
 * Object and Class for the GeoHashSpiral
 *
 * This provides a Iterator[GeoHash] which generates GeoHashes in order from the distance from a single point
 * Currently the ordering is according to the Cartesian distance, NOT the geodetic distance
 * However the provided filter interface does use geodetic distance
 */

trait GeoHashDistanceFilter {

  // distance in meters used by the filter
  var statefulFilterDistance: Double

  // modifies statefulFiilterDistance if a new distance is smaller
  def mutateFilterDistance(theNewMaxDistance: Double) {
    if (theNewMaxDistance < statefulFilterDistance) statefulFilterDistance = theNewMaxDistance
  }
  // removes GeoHashes that are further than a certain distance from a feature or point
  // as long as distance is in cartesian degrees, it needs to be compared to the
  // statefulFilterDistance converted to degrees
  def statefulDistanceFilter(x: GeoHashWithDistance): Boolean = { x.dist < distanceConversion(statefulFilterDistance) }

  // this is the conversion from distance in meters to the maximum distance in degrees
  // this can be removed once GEOMESA-226 is resolved
  def distanceConversion: Double => Double

}
trait GeoHashAutoSize {
  // typically 25 bits are encoded in the Index Key
  val allowablePrecisions = List(25,30,35,40)
  // find the smallest GeoHash whose minimumSize is larger than the desiredSizeInMeters
  def geoHashToSize(pointInside: Point, desiredSizeInMeters: Double ): GeoHash = {
    val variablePrecGH = allowablePrecisions.reverse.map { prec => GeoHash(pointInside,prec) }
    val largeEnoughGH = variablePrecGH.filter { gh => GeohashUtils.getGeohashMinDimensionMeters(gh) > desiredSizeInMeters  }
    largeEnoughGH.head
  }
}

object GeoHashSpiral extends GeoHashAutoSize {
  def apply(centerPoint: SimpleFeature, distanceGuess: Double, maxDistance: Double) = {

    // generate the central GeoHash as a seed with precision/size governed by distanceGuess
    val seedGH = geoHashToSize(centerPoint.point, distanceGuess)
    val seedWithDistance = GeoHashWithDistance(seedGH, 0.0)

    // These are helpers for distance calculations and ordering.
    // FIXME: using JTS distance returns the cartesian distance only, and does NOT handle wraps correctly
    // see GEOMESA-226
    def distanceCalc(gh: GeoHash) = centerPoint.point.distance(gh.geom)

    def orderedGH: Ordering[GeoHashWithDistance] = Ordering.by { _.dist}
    // this can be removed once GEOMESA-226 is resolved
    def metersConversion(meters: Double) =  distanceDegrees(centerPoint.point, meters)

    // Create a new GeoHash PriorityQueue and enqueue with a seed.
    val ghPQ = new mutable.PriorityQueue[GeoHashWithDistance]()(orderedGH.reverse) { enqueue(seedWithDistance) }

    new GeoHashSpiral(ghPQ, distanceCalc, maxDistance, metersConversion)

  }
}

class GeoHashSpiral(pq: mutable.PriorityQueue[GeoHashWithDistance],
                     val distance: (GeoHash) => Double,
                     var statefulFilterDistance: Double,
                     val distanceConversion: (Double) => Double) extends GeoHashDistanceFilter with BufferedIterator[GeoHash] {

  // running set of GeoHashes which have already been encountered -- used to prevent visiting a GeoHash more than once
  // TODO: think if this is needed, or if distance ordering/filtering alone is sufficient
  //       the Best Way will depend on if calculating the distance for old GHs is more expensive than
  //       searching through a HashSet
  val oldGH = new mutable.HashSet[GeoHash] ++= pq.toSet[GeoHashWithDistance].map{ _.gh }

  // these are used to setup a modified on-deck pattern: a PriorityQueue backed by a generator
  var onDeck: Option[GeoHashWithDistance] = None
  var nextGHFromPQ: Option[GeoHashWithDistance] = None

  // prime the on deck pattern
  loadNextGHFromPQ()
  loadNextGHFromTouching()
  loadNext()

  // method used to find the next element in the PQ that passes a filter
  @tailrec
  private def loadNextGHFromPQ() {
    if (pq.isEmpty) nextGHFromPQ = None
    else {
      val theHead = pq.dequeue()  // removes elements from pq

      if (statefulDistanceFilter(theHead)) nextGHFromPQ = Option(theHead)

      else loadNextGHFromPQ()
    }
  }
  // method to load the neighbors of the next GeoHash to be visited into the PriorityQueue
  private def loadNextGHFromTouching() {
    // use the GeoHash already taken from the head of the PriorityQueue as a seed
    nextGHFromPQ.foreach { newSeedGH =>
      // obtain only *new* GeoHashes that touch
      val newTouchingGH = TouchingGeoHashes.touching(newSeedGH.gh).filterNot(oldGH contains)

      // enrich the GeoHashes with distances
      val withDistance = newTouchingGH.map { aGH => GeoHashWithDistance(aGH, distance(aGH))}

      // remove the GeoHashes that are located too far away
      val withinDistance = withDistance.filter(statefulDistanceFilter)

      // add all GeoHashes which pass the filter to the PQ
      withinDistance.foreach { ghWD => pq.enqueue(ghWD)}

      // also add the new GeoHashes to the set of old GeoHashes
      // note: we add newTouchingGH now, since the cost of having many extra GeoHashes will likely
      // be less than that of computing the distance for the same GeoHash multiple times,
      // which is what happens if withinDistance is used.
      oldGH ++= newTouchingGH
    }
  }
  // loads the head element in the PQ into onDeck, and adds GeoHashes to the PQ
  private def loadNext() {
    nextGHFromPQ match {
      case (None) => onDeck = None // nothing left in the priorityQueue
      case (Some(x)) => loadNextGHFromPQ(); loadNextGHFromTouching(); onDeck = Some(x)
    }
  }

  // filter applied here to account for mutations in the filter AFTER onDeck is loaded
  def head = onDeck.filter(statefulDistanceFilter) match {
    case Some(nextGH) => nextGH.gh
    case None => throw new Exception
  }
  // filter applied here to account for mutations in the filter AFTER onDeck is loaded
  def hasNext = onDeck.filter(statefulDistanceFilter).isDefined

  def next() = head match {case nextGH:GeoHash => loadNext() ; nextGH }

}