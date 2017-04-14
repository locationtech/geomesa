package org.locationtech.geomesa.hbase.filters

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.{Filter, FilterBase}
import org.locationtech.geomesa.index.filters.Z3Filter

class Z3HBaseFilter(filt: Z3Filter) extends FilterBase with LazyLogging {

  override def filterKeyValue(v: Cell): Filter.ReturnCode = {
    logger.trace("In filterKeyValue()")
    if(filt.inBounds(v.getRowArray, v.getRowOffset, v.getRowLength)) Filter.ReturnCode.INCLUDE
    else Filter.ReturnCode.SKIP
  }

  override def toByteArray: Array[Byte] = {
    logger.trace("Serializing Z3HBaseFilter")
    Z3Filter.toByteArray(filt)
  }


}

object Z3HBaseFilter extends LazyLogging {
  @throws[DeserializationException]
  def parseFrom(pbBytes: Array[Byte]): Filter = {
    logger.debug("Deserializing Z3HBaseFilter")
    new Z3HBaseFilter(Z3Filter.fromByteArray(pbBytes))
  }

}
