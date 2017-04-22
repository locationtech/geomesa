package org.locationtech.geomesa.hbase.filters

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.{Filter, FilterBase}
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.locationtech.geomesa.index.filters.{Z2Filter, Z3Filter}

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


class Z2HBaseFilter(filt: Z2Filter) extends FilterBase with LazyLogging {

  override def filterKeyValue(v: Cell): Filter.ReturnCode = {
    // TODO: can we avoid the clone?
    logger.trace("In filterKeyValue()")
    if(filt.inBounds(CellUtil.cloneRow(v))) Filter.ReturnCode.INCLUDE
    else Filter.ReturnCode.SKIP
  }

  override def toByteArray: Array[Byte] = {
    logger.trace("Serializing Z2HBaseFilter")
    Z2Filter.toByteArray(filt)
  }


}

object Z2HBaseFilter extends LazyLogging {
  @throws[DeserializationException]
  def parseFrom(pbBytes: Array[Byte]): Filter = {
    logger.debug("Deserializing Z2HBaseFilter")
    new Z2HBaseFilter(Z2Filter.fromByteArray(pbBytes))
  }

}
