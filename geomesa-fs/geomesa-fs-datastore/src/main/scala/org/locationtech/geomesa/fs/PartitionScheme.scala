package org.locationtech.geomesa.fs

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Date

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case class Partition(name: String)

trait PartitionScheme {
  def getPartition(sf: SimpleFeature): Partition
}

class DatePartitionScheme(fmt: DateTimeFormatter, sft: SimpleFeatureType, partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    Partition(fmt.format(instant))
  }
}

class IntraHourPartitionScheme(minuteIntervals: Int, fmt: DateTimeFormatter, sft: SimpleFeatureType, partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    val adjusted = instant.withMinute(minuteIntervals*instant.getMinute/minuteIntervals)
    Partition(fmt.format(adjusted))
  }
}

class HierarchicalPartitionScheme(partitionSchemes: Seq[PartitionScheme], sep: String) extends PartitionScheme {
  override def getPartition(sf: SimpleFeature): Partition =
    Partition(partitionSchemes.map(_.getPartition(sf).name).mkString(sep))
}

object PartitionScheme {

  def apply(): PartitionScheme = ???
}
