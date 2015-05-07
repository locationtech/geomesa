package org.locationtech.geomesa.core.data.tables

import java.util.Date

import com.google.common.base.Charsets
import com.google.common.collect.ImmutableSet
import com.google.common.primitives.{Bytes, Longs, Shorts}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.io.Text
import org.calrissian.mango.types.LexiTypeEncoders
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.core.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.core.index
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.`type`.GeometryDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object Z3Table {

  val EPOCH = new DateTime(0)
  val SFC = new Z3SFC
  val BIN_ROW = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val EMPTY_TEXT = new Text(EMPTY_BYTES)

  def z3writer(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex =
      index.getDtgDescriptor(sft)
        .map { desc => sft.indexOf(desc.getName) }
        .getOrElse { throw new IllegalArgumentException("Must have a date for a Z3 index")}


    (fw: FeatureToWrite) => {
      val geom = fw.feature.point
      val x = geom.getX
      val y = geom.getY
      val dtg = new DateTime(fw.feature.getAttribute(dtgIndex).asInstanceOf[Date])
      val weeks = Weeks.weeksBetween(EPOCH, new DateTime(dtg))
      val prefix = Shorts.toByteArray(weeks.getWeeks.toShort)
      val secondsInWeek = Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds
      val z3 = SFC.index(x, y, secondsInWeek)
      val z3idx = Longs.toByteArray(z3.z)

      val idBytes = fw.feature.getID.getBytes(Charsets.UTF_8)
      
      val row = Bytes.concat(prefix, z3idx, idBytes)
      val m = new Mutation(row)

      val attrToIndex = getAttributesToIndex(sft)
      attrToIndex.foreach { case (d, idx) =>
        val lexi = fw.feature.getAttribute(idx) match {
          case l: java.lang.Integer  => LexiTypeEncoders.LEXI_TYPES.encode(Int.box(l))
          case d: java.lang.Double   => LexiTypeEncoders.LEXI_TYPES.encode(Double.box(d))
          case t                     => LexiTypeEncoders.LEXI_TYPES.encode(t)
        }
        val cq = Bytes.concat(lexi.getBytes(Charsets.UTF_8))
        m.put(d, new Text(cq), fw.columnVisibility, fw.dataValue)
      }
      m.put(BIN_ROW, EMPTY_TEXT, fw.columnVisibility, EMPTY_VALUE)
      Seq(m)      
    }

  }

  def configureTable(sft: SimpleFeatureType, z3Table: String, tableOps: TableOperations): Unit = {
    import scala.collection.JavaConversions._

    val indexedAttributes = getAttributesToIndex(sft)
    val localityGroups: Map[Text, Text] =
      indexedAttributes.map { case (name, _) => (name, name) }.toMap.+((BIN_ROW, BIN_ROW))
    tableOps.setLocalityGroups(z3Table, localityGroups.map { case (k, v) => (k.toString, ImmutableSet.of(v)) } )
  }

  private def getAttributesToIndex(sft: SimpleFeatureType) =
    sft.getAttributeDescriptors
      .filterNot { d => d.isInstanceOf[GeometryDescriptor] }
      .map { d => (new Text(d.getLocalName.getBytes(Charsets.UTF_8)), sft.indexOf(d.getName)) }
}
