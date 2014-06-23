package geomesa.core.filter

import org.geotools.filter.visitor.DefaultFilterVisitor
import org.opengis.filter._
import scala.collection.JavaConversions._

// This class helps us split a Filter into pieces if there are ORs at the top.
class OrSplittingFilter extends DefaultFilterVisitor {

  // This function really returns a Seq[Filter].
  override def visit(filter: Or, data: scala.Any): AnyRef = {
    filter.getChildren.flatMap { subfilter =>
      this.visit(subfilter, data)
    }
  }

  def visit(filter: Filter, data: scala.Any): Seq[Filter] = {
    filter match {
      case o: Or => visit(o, data).asInstanceOf[Seq[Filter]]
      case _     => Seq(filter)
    }
  }
}
