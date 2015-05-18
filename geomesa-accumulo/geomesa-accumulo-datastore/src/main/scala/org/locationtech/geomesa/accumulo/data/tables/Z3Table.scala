package org.locationtech.geomesa.accumulo.data.tables

import java.nio.ByteBuffer
import java.util.Date
import java.util.Map.Entry

import com.google.common.base.Charsets
import com.google.common.collect.ImmutableSet
import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.vividsolutions.jts.geom.Point
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.index
import org.locationtech.geomesa.accumulo.index.QueryPlanners._
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.feature.nio.{AttributeAccessor, LazySimpleFeature}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.`type`.{GeometryType, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object Z3Table {

  val EPOCH = new DateTime(0)
  val SFC = new Z3SFC
  val FULL_ROW = new Text("F")
  val BIN_ROW = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val EMPTY_TEXT = new Text(EMPTY_BYTES)

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def z3writer(sft: SimpleFeatureType): FeatureToMutations = {
    val dtgIndex =
      index.getDtgDescriptor(sft)
        .map { desc => sft.indexOf(desc.getName) }
        .getOrElse { throw new IllegalArgumentException("Must have a date for a Z3 index")}
    if (sft.getGeometryDescriptor.getType.getBinding != classOf[Point]) {
      // TODO more robust method for not using z3 with non-point geoms
      // TODO also see org.locationtech.geomesa.accumulo.index.Z3IdxStrategy.getStrategy
      return (fw: FeatureToWrite) => Seq.empty
    }
    val writer = new KryoFeatureSerializer(sft)
    (fw: FeatureToWrite) => {
      val bytesWritten = writer.serialize(fw.feature)
      val payload = new Value(bytesWritten)
      val geom = fw.feature.point
      val x = geom.getX
      val y = geom.getY
      val dtg = new DateTime(fw.feature.getAttribute(dtgIndex).asInstanceOf[Date])
      val weeks = epochWeeks(dtg)
      val prefix = Shorts.toByteArray(weeks.getWeeks.toShort)
      val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
      val z3 = SFC.index(x, y, secondsInWeek)
      val z3idx = Longs.toByteArray(z3.z)

      val idBytes = fw.feature.getID.getBytes(Charsets.UTF_8)
      
      val row = Bytes.concat(prefix, z3idx, idBytes)
      val m = new Mutation(row)
/*
      val attrToIndex = getAttributesToIndex(sft)
      attrToIndex.foreach { case (d, idx) =>
        val lexi = fw.feature.getAttribute(idx) match {
          case l: java.lang.Integer  => LexiTypeEncoders.LEXI_TYPES.encode(Int.box(l))
          case d: java.lang.Double   => LexiTypeEncoders.LEXI_TYPES.encode(Double.box(d))
          case null                  => null
          case t                     => LexiTypeEncoders.LEXI_TYPES.encode(t)

        }
        if(lexi != null) {
          val cq = lexi.getBytes(Charsets.UTF_8)
          m.put(d, new Text(cq), fw.columnVisibility, payload)
        }
      }
*/
      m.put(BIN_ROW, EMPTY_TEXT, fw.columnVisibility, EMPTY_VALUE)
      m.put(FULL_ROW, EMPTY_TEXT, fw.columnVisibility, payload)
      Seq(m)
    }

  }

  def adaptZ3Iterator(sft: SimpleFeatureType): FeatureFunction = {
    val accessors = AttributeAccessor.buildSimpleFeatureTypeAttributeAccessors(sft)
    val fn = (e: Entry[Key, Value]) => {
      val k = e.getKey
      val row = k.getRow.getBytes
      val idbytes = row.slice(10, Int.MaxValue)
      val id = new String(idbytes)
      new LazySimpleFeature(id, sft, accessors, ByteBuffer.wrap(e.getValue.get()))
      // TODO visibility
    }
    Left(fn)
  }

  def adaptZ3KryoIterator(sft: SimpleFeatureType): FeatureFunction = {
    val kryo = new KryoFeatureSerializer(sft)
    val fn = (e: Entry[Key, Value]) => {
      kryo.deserialize(e.getValue.get())
    }
    Left(fn)
  }

  /*
  private val Z3CURVE = new Z3SFC
  private val gt = JTSFactoryFinder.getGeometryFactory
  def adaptZ3Iterator(iter: KVIter, query: Query): SFIter = {
    val ft = SimpleFeatureTypes.createType(query.getTypeName, "dtg:Date,geom:Point:srid=4326")
    val builder = new SimpleFeatureBuilder(ft)
    iter.map { e =>
      val k = e.getKey
      val row = k.getRow.getBytes
      val weekBytes = row.slice(0, 2)
      val zbytes = row.slice(2, 10)
      val idbytes = row.slice(10, Int.MaxValue)

      val id = new String(idbytes)
      val zvalue = Longs.fromByteArray(zbytes)
      val z = Z3(zvalue)
      val (x, y, t) = Z3CURVE.invert(z)
      val pt = gt.createPoint(new Coordinate(x, y))
      val week = Shorts.fromByteArray(weekBytes)
      val seconds = week * Weeks.ONE.toStandardSeconds.getSeconds + Seconds.seconds(t.toInt).getSeconds

      val dtg = new DateTime(seconds * 1000L)
      builder.reset()
      builder.addAll(Array[AnyRef](dtg, pt))
      builder.buildFeature(id)
    }
  }
*/

  def configureTable(sft: SimpleFeatureType, z3Table: String, tableOps: TableOperations): Unit = {
    import scala.collection.JavaConversions._

    val indexedAttributes = getAttributesToIndex(sft)
    val localityGroups: Map[Text, Text] =
      indexedAttributes.map { case (name, _) => (name, name) }.toMap.+((BIN_ROW, BIN_ROW)).+((FULL_ROW, FULL_ROW))
    tableOps.setLocalityGroups(z3Table, localityGroups.map { case (k, v) => (k.toString, ImmutableSet.of(v)) } )
  }

  private def getAttributesToIndex(sft: SimpleFeatureType) =
    sft.getAttributeDescriptors
      .filterNot { d => d.isInstanceOf[GeometryDescriptor] }
      .map { d => (new Text(d.getLocalName.getBytes(Charsets.UTF_8)), sft.indexOf(d.getName)) }
}
