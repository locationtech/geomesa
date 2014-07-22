package geomesa.core

import org.geotools.factory.CommonFactoryFinder
import org.opengis.filter._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal.BinaryTemporalOperator
import scala.collection.JavaConversions._

package object filter {
  // Claim: FilterFactory implementations seem to be thread-safe away from
  //  'namespace' and 'function' calls.
  // As such, we can get away with using a shared Filter Factory.
  implicit val ff = CommonFactoryFinder.getFilterFactory2

  /**
   * This function rewrites a org.opengis.filter.Filter in terms of a top-level OR with children filters which
   * 1) do not contain further ORs, (i.e., ORs bubble up)
   * 2) only contain at most one AND which is at the top of their 'tree'
   *
   * Note that this further implies that NOTs have been 'pushed down' and do have not have ANDs nor ORs as children.
   *
   * In boolean logic, this form is called disjunctive normal form (DNF).
   *
   * @param filter An arbitrary filter.
   * @return       A filter in DNF (described above).
   */
  def rewriteFilter(filter: Filter)(implicit ff: FilterFactory): Filter = {
    val ll =  logicDistribution(filter)
    if(ll.size == 1) {
      if(ll(0).size == 1) ll(0)(0)
      else ff.and(ll(0))
    }
    else  {
      val children = ll.map { l =>
        l.size match {
          case 1 => l(0)
          case _ => ff.and(l)
        }
      }
      ff.or(children)
    }
  }

  /**
   *
   * @param x: An arbitrary @org.opengis.filter.Filter
   * @return   A List[List[Filter]] where the inner List of Filters are to be joined by
   *           Ands and the outer list combined by Ors.
   */
  private[core] def logicDistribution(x: Filter): List[List[Filter]] = x match {
    case or: Or  => or.getChildren.toList.flatMap(logicDistribution)

    case and: And => and.getChildren.foldRight (List(List.empty[Filter])) {
      (f, dnf) => for {
        a <- logicDistribution (f)
        b <- dnf
      } yield a ++ b
    }

    case not: Not =>
      not.getFilter match {
        case and: And => logicDistribution(deMorgan(and))
        case or:  Or => logicDistribution(deMorgan(or))
        case f: Filter => List(List(not))
      }

    case f: Filter => List(List(f))
  }

  /**
   *  The input is a filter which had a Not applied to it.
   *  This function uses deMorgan's law to 'push the Not down'
   *   as well as cancel adjacent Nots.
   */
  private[core] def deMorgan(f: Filter)(implicit ff: FilterFactory): Filter = f match {
    case and: And => ff.or(and.getChildren.map(a => ff.not(a)))
    case or:  Or  => ff.and(or.getChildren.map(a => ff.not(a)))
    case not: Not => not.getFilter
  }

  // Takes a filter and returns a Seq of Geometric/Topological filters under it.
  //  As a note, currently, only 'good' filters are considered.
  //  The list of acceptable filters is defined by 'spatialFilters'
  //  The notion of 'good' here means *good* to handle to the STII.
  //  Of particular note, we should not give negations to the STII.
  def partitionSubFilters(filter: Filter, filterFilter: Filter => Boolean): (Seq[Filter], Seq[Filter]) = {
    filter match {
      case a: And => a.getChildren.partition(filterFilter)
      case _ => Seq(filter).partition(filterFilter)
    }
  }

  def partitionGeom(filter: Filter) = partitionSubFilters(filter, spatialFilters)

  def partitionTemporal(filters: Seq[Filter]): (Seq[Filter], Seq[Filter]) = filters.partition(temporalFilters)

  // Defines the topological predicates we like for use in the STII.
  def spatialFilters(f: Filter): Boolean = {
    f match {
      case _: BBOX => true
      case _: Contains => true
      case _: Crosses => true
      case _: Intersects => true
      case _: Overlaps => true
      case _: Within => true
      case _ => false        // Beyond, Disjoint, DWithin, Equals, Touches
    }
  }

  // Notes: This may need to be 'smaller' as we may wish to handle the various temporal predicates more carefully.
  //  Also, this needs to cover 'BETWEEN' with the indexed date field.
  def temporalFilters(f: Filter): Boolean = f.isInstanceOf[BinaryTemporalOperator]

}
