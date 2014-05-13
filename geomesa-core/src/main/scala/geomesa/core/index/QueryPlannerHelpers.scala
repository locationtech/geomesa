package geomesa.core.index

import com.vividsolutions.jts.geom.Polygon
import org.joda.time.Interval

class QueryPlannerHelpers {

}


object ApportionedRanges {
  // these are priority values for Accumulo iterators
  val HIGHEST_ITERATOR_PRIORITY = 0
  val LOWEST_ITERATOR_PRIORITY = 1000

  /**
   * Given a range of integers, return a uniformly-spaced sample whose count matches
   * the desired quantity.  The end-points of the range should be inclusive.
   *
   * @param minValue the minimum value of the range of integers
   * @param maxValue the maximum value of the range of integers
   * @param rawNumItems the number of points to allocate
   * @return the points uniformly spaced over this range
   */
  def apportionRange(minValue: Int = 0, maxValue: Int = 100, rawNumItems: Int = 3) : Seq[Int] = {
    val numItems = scala.math.max(rawNumItems, 1)
    if (minValue==maxValue) List.fill(numItems)(minValue)
    else {
      val span = maxValue - minValue
      (1 to numItems).map(n => scala.math.round((n-1.0)/(numItems-1.0)*span+minValue).toInt)
    }
  }
}

import ApportionedRanges._

trait ApportionedRanges {
  // the order in which the various iterators are applied depends upon their
  // priority values:  lower priority-values run earlier, and higher priority-
  // values will run later; here, we visually assign priorities from highest
  // to lowest so that the first item in this list will run first, and the last
  // item in the list will run last
  val Seq(
  iteratorPriority_RowRegex, // highest priority:  runs first
  iteratorPriority_ColFRegex,
  iteratorPriority_SpatioTemporalIterator,
  iteratorPriority_AttributeAggregator,
  iteratorPriority_SimpleFeatureFilteringIterator  // lowest priority:  runs last
  ) = apportionRange(HIGHEST_ITERATOR_PRIORITY, LOWEST_ITERATOR_PRIORITY, 5)

}