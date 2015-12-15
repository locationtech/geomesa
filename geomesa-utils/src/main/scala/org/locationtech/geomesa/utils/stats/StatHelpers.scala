package org.locationtech.geomesa.utils.stats

import java.text.SimpleDateFormat

import org.joda.time.format.DateTimeFormat

object StatHelpers {
  val javaDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
}