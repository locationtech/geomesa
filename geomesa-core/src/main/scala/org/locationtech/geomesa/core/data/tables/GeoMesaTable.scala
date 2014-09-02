package org.locationtech.geomesa.core.data.tables

import org.apache.accumulo.core.client.{BatchDeleter, Connector}
import org.apache.accumulo.core.data
import org.locationtech.geomesa.core.index._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

trait GeoMesaTable {

  def deleteFeaturesFromTable(conn: Connector, bd: BatchDeleter, sft: SimpleFeatureType): Unit = {
    val MIN_START = "\u0000"
    val MAX_END = "~"

    val prefix = getTableSharingPrefix(sft)

    val range = new data.Range(prefix + MIN_START, prefix + MAX_END)

    println(s"Deleting for range $range")

    bd.setRanges(Seq(range))
    bd.delete()
    bd.close()
  }

}
