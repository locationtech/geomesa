package org.locationtech.geomesa.accumulo.data.tables

import java.nio.charset.StandardCharsets

import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToMutations
import org.opengis.feature.simple.SimpleFeatureType
import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, LineString, Point}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.sfcurve.zorder.Z2
import org.locationtech.sfcurve.zorder.Z3.ZPrefix
import org.opengis.feature.simple.SimpleFeatureType


object SSIZ2Table extends GeoMesaTable {

  val FULL_CF = new Text("F")
  val BIN_CF = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq

  /**
    * Is the table compatible with the given feature type
    */
  // TODO Coordinate the schemaVersion check with the values in the
  // org.locationtech.geomesa package object
  // TODO return false on geom:Point SFTs
  override def supports(sft: SimpleFeatureType): Boolean =
  sft.getGeometryDescriptor != null && sft.getSchemaVersion > 8 // && NOT ONLY POINTS!

  /**
    * The name used to identify the table
    */
  override def suffix: String = "ssiz2"

  /**
    * Creates a function to write a feature to the table
    */
  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val sharing = sharingPrefix(sft)
    val getRowKeys: (FeatureToWrite) => Seq[Array[Byte]] = calculateRowKeys(sharing)

    sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        (fw: FeatureToWrite) => {
          val rows = getRowKeys(fw)
          // store the duplication factor in the column qualifier for later use
          val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
          rows.map { row =>
            val mutation = new Mutation(row)
            mutation.put(FULL_CF, cq, fw.columnVisibility, fw.dataValue)
            fw.binValue.foreach(v => mutation.put(BIN_CF, cq, fw.columnVisibility, v))
            mutation
          }
        }
      case VisibilityLevel.Attribute =>
        (fw: FeatureToWrite) => {
          val rows = getRowKeys(fw)
          // TODO GEOMESA-1254 duplication factor, bin values
          rows.map { row =>
            val mutation = new Mutation(row)
            fw.perAttributeValues.foreach(key => mutation.put(key.cf, key.cq, key.vis, key.value))
            mutation
          }
        }
    }
  }

  /**
    * Creates a function to delete a feature to the table
    */
  override def remover(sft: SimpleFeatureType): FeatureToMutations = ???

  def calculateRowKeys(tableSharing: Array[Byte])(ftw: FeatureToWrite): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    val split: Array[Byte] = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val id = ftw.feature.getID.getBytes(StandardCharsets.UTF_8)
    val geom = ftw.feature.getDefaultGeometry.asInstanceOf[Geometry]
    val indexBytes: Array[Byte] = SSIZ2SFC.indexGeometryToBytes(geom)
    Seq(Bytes.concat(tableSharing, split, indexBytes, id))
  }

  /**
    * Retrieve an ID from a row. All tables are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row
    */
  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = ???



  private def sharingPrefix(sft: SimpleFeatureType): Array[Byte] = {
    val sharing = if (sft.isTableSharing) {
      sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
    } else {
      Array.empty[Byte]
    }
    require(sharing.length < 2, s"Expecting only a single byte for table sharing, got ${sft.getTableSharingPrefix}")
    sharing
  }

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    // TODO: Set locality groups
    // TODO: Set Splits
  }
 }
