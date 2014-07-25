package geomesa.core.process.knn

import geomesa.utils.geohash.VincentyModel
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable


trait NearestNeighbors[T] extends BoundedPriorityQueue[T] {

  def distance(sf: SimpleFeature): Double

  def maxDistance: Option[Double]

  implicit def toSimpleFeatureWithDistance(sf: SimpleFeature): (SimpleFeature, Double) = (sf, distance(sf))

  implicit def backToSimpleFeature(sfTuple: (SimpleFeature, Double)): SimpleFeature = sfTuple._1
}

object NearestNeighbors {
  def apply(aFeatureForSearch: SimpleFeature, numDesired: Int) = {
    //def distanceCalc(geom: Geometry) = aFeatureForSearch.point.distance(geom)

    def distanceCalc(sf: SimpleFeature) =
      VincentyModel.getDistanceBetweenTwoPoints(aFeatureForSearch.point, sf.point).getDistanceInMeters

    def orderedSF: Ordering[(SimpleFeature, Double)] =
      Ordering.by { sfTuple: (SimpleFeature, Double) => sfTuple._2}.reverse

    new mutable.PriorityQueue[(SimpleFeature, Double)]()(orderedSF) with NearestNeighbors[(SimpleFeature, Double)] {

      val maxSize = numDesired
      // needed to make IDEA happy, but scalac is fine without this.
      //override val ord = orderedSF

      def distance(sf: SimpleFeature) = distanceCalc(sf)

      def maxDistance = getLast.map {_._2}
    }
  }
}
  // this should include a guard against adding two NearestNeighbor collections which are for different points
  // override def ++ (that: NearestNeighbors ) =  that.dequeueAll
  // should override enqueue to prevent more than k elements from being contained
