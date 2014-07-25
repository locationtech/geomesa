package geomesa.core.process.knn

import collection.JavaConverters._
import geomesa.core.process.knn.EnrichmentPatch._
import geomesa.utils.geohash._
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Object and Class to mock up a generator for the GeoHash "Spiral"
 *
 * For now, we use the BoundindBoxIterator as the GeoHash iterator.
 * Later, this will switch to the Touching GeoHash Iterator
 */
trait NearestGeoHash {
  def distance: GeoHash => Double
}

trait GeoHashDistanceFilter extends NearestGeoHash {
  var statefulFilterRadius: Double
  // removes GeoHashes that are further than a certain distance from the aFeatureForSearch
  def statefulDistanceFilter(gh: GeoHash): Boolean = { distance(gh) < statefulFilterRadius }
  // FIXME
  // from jnh5y: Sans a call to this method, I'd advocate removing it, making the var a val, and moving def distance into this trait.
  def mutateFilterRadius(radiusCandidate: Double): Unit = 
  {statefulFilterRadius = math.min(radiusCandidate, statefulFilterRadius)}
}

object SomeGeoHashes {
  def apply(centerPoint: SimpleFeature, distanceGuess: Double, maxDistance: Double) = {

    // take the center point and use the supplied bounds..
    val (llGH, urGH) = GeoHashIterator.getBoundingGeoHashes(List(centerPoint.point), 30, maxDistance)
    val bBox = TwoGeoHashBoundingBox(llGH, urGH)
    val ghIt = new BoundingBoxGeoHashIterator(bBox).asScala

    // These are helpers for distance calculations and ordering.
    // FIXME: using JTS distance returns the cartesian distance only, and does NOT handle wraps correctly
    // also, the units are degrees, while meters are used elsewhere. So this won't even work.
    // see GEOMESA-226

    def distanceCalc(gh: GeoHash) = centerPoint.point.distance(GeohashUtils.getGeohashGeom(gh))
    def orderedGH: Ordering[GeoHash] = Ordering.by { gh: GeoHash => distanceCalc(gh)}

    // Create a new GeoHash PriorityQueue and enqueue the first GH from the iterator as a seed.
    val ghPQ = new mutable.PriorityQueue[GeoHash]()(orderedGH) { enqueue(ghIt.next()) }
    new SomeGeoHashes(ghPQ, ghIt, distanceCalc, maxDistance)
  }
}
// from jnh5y: this should just implement Iterator
class SomeGeoHashes(pq: mutable.PriorityQueue[GeoHash],
                     it: Iterator[GeoHash],
                     val distance: (GeoHash) => Double,
                     var statefulFilterRadius: Double  ) extends GeoHashDistanceFilter {

  def updateDistance(theNewMaxDistance: Double) {
    if (theNewMaxDistance < statefulFilterRadius) statefulFilterRadius = theNewMaxDistance
  }

  // Note that next returns an Option. There is then no need to define hasNext.
  def next(): Option[GeoHash] =
    for {
      newGH <- pq.dequeuingFind { statefulDistanceFilter } // get the next element in the queue that passes the filter
      _      = it.find { statefulDistanceFilter } foreach {gh:GeoHash => pq.enqueue(gh)} // insert the same from the iterator
    } yield newGH

  def toList(): List[GeoHash] = {getNext(List[GeoHash]()) }
  @tailrec
  private def getNext(ghList: List[GeoHash]): List[GeoHash] = {
      next() match {
        case None => ghList
        case Some(element) => getNext(element::ghList)
      }
    }
  // this method is used to load the PriorityQueue with contents from the iterator
  // this is needed in this case since the iterator only returns results one at a time and hence
  // next() will not return results in order
  @tailrec
  final def exhaustIterator(): Unit = {
    it.find { statefulDistanceFilter } match {
      case None => Unit
      case Some(element) => pq.enqueue(element); exhaustIterator()
    }
  }
}

object EnrichmentPatch {
  // It might be nice to make this more general and dispense with the GeoHash type.
  implicit class EnrichedPQ[A](pq: mutable.PriorityQueue[A]) {
    @tailrec
    final def dequeuingFind(func: A => Boolean): Option[A] = {
      if (pq.isEmpty) None
      else {
        val theHead = pq.dequeue()
        if (func(theHead)) Option(theHead)
        else dequeuingFind(func)
      }
    }
  }

  /**
  implicit class EnrichedBBGHI(bbghi: BoundingBoxGeoHashIterator) {
    final def find(func: GeoHash => Boolean): Option[GeoHash] = {
      if (!bbghi.hasNext) None
      else {
        val theHead = bbghi.next()
        if (func(theHead)) Option(theHead)
        else bbghi.find(func)
      }
    }
  }
  **/
}