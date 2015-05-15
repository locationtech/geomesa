package org.locationtech.geomesa.accumulo.filter

import org.geotools.filter.visitor.DefaultFilterVisitor
import org.opengis.filter.{And, Filter}

import scala.collection.JavaConversions._

// This class helps us split a Filter into pieces if there are ANDs at the top.
class AndSplittingFilter extends DefaultFilterVisitor {

  // This function really returns a Seq[Filter].
  override def visit(filter: And, data: scala.Any): AnyRef = {
    filter.getChildren.flatMap { subfilter =>
      this.visit(subfilter, data)
    }
  }

  def visit(filter: Filter, data: scala.Any): Seq[Filter] = {
    filter match {
      case a: And => visit(a, data).asInstanceOf[Seq[Filter]]
      case _     => Seq(filter)
    }
  }
}
