package org.locationtech.geomesa.arrow.data

import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.{Filter, FilterVisitor, PropertyIsEqualTo}

/**
  * Created by anthony on 5/6/17.
  */
object FilterOptimizer {
  private val ff = CommonFactoryFinder.getFilterFactory2

  def rewrite(f: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = f match {
    case e: PropertyIsEqualTo => rewritePropertyIsEqualTo(e, sft, dictionaries)
    case _ => f
  }

  def rewritePropertyIsEqualTo(e: PropertyIsEqualTo, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = {
    // TODO: pass dictionaries around with better attribute names rather than 'actor1Name:String'
    (e.getExpression1, e.getExpression2) match {
      case (left: PropertyName, right: Literal) if dictionaries.get(s"${left.getPropertyName}:String").isDefined =>
        val attrIndex = sft.indexOf(left.getPropertyName)
        val numericValue = dictionaries(s"${left.getPropertyName}:String").index(right.getValue)
        FastEquals(numericValue, attrIndex)
    }
  }

  case class FastEquals(v: Int, attrIndex: Int) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData

    override def evaluate(o: AnyRef): Boolean = {
      o.asInstanceOf[ArrowSimpleFeature].getAttributeRaw(attrIndex).asInstanceOf[Short] == v
    }
  }
}
