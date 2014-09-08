package org.locationtech.geomesa.core.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.opengis.filter.Filter

object FilterUtils {

  val ff = CommonFactoryFinder.getFilterFactory2

  implicit class RichFilter(val filter: Filter) {
    def &&(that: Filter) = ff.and(filter, that)

    def ||(that: Filter) = ff.or(filter, that)

    def ! = ff.not(filter)
  }

  implicit def stringToFilter(s: String) = ECQL.toFilter(s)

  implicit def intToAttributeFilter(i: Int): Filter = s"attr$i = val$i"

  implicit def intToFilter(i: Int): RichFilter = intToAttributeFilter(i)

}