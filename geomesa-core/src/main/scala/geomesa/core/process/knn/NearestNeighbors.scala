package geomesa.core.process.knn

import geomesa.utils.geohash.VincentyModel
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature


trait NearestNeighbors {
  def distance(sf: SimpleFeature): Double

  def maxDistance: Option[Double]

  implicit def toSimpleFeatureWithDistance(sf: SimpleFeature): (SimpleFeature, Double) = (sf, distance(sf))

  implicit def backToSimpleFeature(sfTuple: (SimpleFeature, Double)): SimpleFeature = sfTuple._1

}

object NearestNeighbors {
  def apply(aFeatureForSearch: SimpleFeature, numDesired: Int) = {

    def distanceCalc(sf: SimpleFeature) =
      VincentyModel.getDistanceBetweenTwoPoints(aFeatureForSearch.point, sf.point).getDistanceInMeters

    def orderedSF: Ordering[(SimpleFeature, Double)] =
      Ordering.by { sfTuple: (SimpleFeature, Double) => sfTuple._2}.reverse

    // type aliased to  BoundedNearestNeighbors
    new BoundedPriorityQueue[(SimpleFeature, Double)](numDesired)(orderedSF) with NearestNeighbors {

      def distance(sf: SimpleFeature) = distanceCalc(sf)

      def maxDistance = Option(last).map {_._2}
    }
  }
}
  // this should include a guard against adding two NearestNeighbor collections which are for different points

