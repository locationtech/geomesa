package org.locationtech.geomesa.utils.stats

import org.joda.time.format.DateTimeFormat

object StatHelpers {
  // for going from a Date's toString to a Date
  val javaDateFormat = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss z yyyy")

  // for going from a Date string formatted as what a SimpleFeature would have to a Date
  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
}