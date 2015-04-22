package org.locationtech.geomesa.security

import org.apache.accumulo.core.security.{ColumnVisibility, VisibilityEvaluator}
import org.locationtech.geomesa.security
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.{Filter, FilterVisitor}

class VisibilityFilter(ve: VisibilityEvaluator) extends Filter {
  import org.locationtech.geomesa.security._

  private val vizCache = collection.mutable.HashMap.empty[String, Boolean]

  override def evaluate(o: Any): Boolean = {
    val viz = o.asInstanceOf[SimpleFeature].visibility
    viz.exists(v =>
      vizCache.getOrElseUpdate(v, ve.evaluate(new ColumnVisibility(v))))
  }

  override def accept(filterVisitor: FilterVisitor, o: AnyRef): AnyRef = o
}

object VisibilityFilter {
  import scala.collection.JavaConversions._

  def apply(): VisibilityFilter = {
    val provider = security.getAuthorizationsProvider(Map.empty[String, Serializable], Seq())
    val auths = provider.getAuthorizations
    val vizEvaluator = new VisibilityEvaluator(auths)
    new VisibilityFilter(vizEvaluator)
  }

}

