/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeature.RowValue
import org.locationtech.geomesa.accumulo.index.AccumuloColumnGroups
import org.locationtech.geomesa.accumulo.index.encoders.{BinEncoder, IndexValueEncoder}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.security.SecurityUtils._
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Feature for writing to accumulo
  */
trait AccumuloFeature extends WrappedFeature {

  /**
    * Main data values
    *
    * @return
    */
  def fullValues: Seq[RowValue]

  def fullValuesWithId: Seq[RowValue]

  /**
    * Index values - e.g. a trimmed down feature with only date and geometry
    *
    * @return
    */
  def indexValues: Seq[RowValue]

  def indexValuesWithId: Seq[RowValue]

  /**
    * Pre-computed BIN values
    *
    * @return
    */
  def binValues: Seq[RowValue]
}

object AccumuloFeature {

  import org.locationtech.geomesa.accumulo.index.AccumuloColumnGroups.{AttributeColumnFamily, BinColumnFamily, IndexColumnFamily}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val EmptyColumnQualifier = new Text()

  def wrapper(sft: SimpleFeatureType, defaultVisibility: String): (SimpleFeature) => AccumuloFeature = {
    val serializerWithId = KryoFeatureSerializer(sft)
    val idSerializer = GeoMesaFeatureIndex.idToBytes(sft)
    val indexSerializer = IndexValueEncoder(sft)
    val indexSerializerWithId = IndexValueEncoder(sft, includeIds = true)
    val binEncoder = BinEncoder(sft)

    sft.getVisibilityLevel match {
      case VisibilityLevel.Feature =>
        val serializers = AccumuloColumnGroups.serializers(sft)
        (sf) => new AccumuloFeatureLevelFeature(sf, defaultVisibility, serializers, serializerWithId,
          idSerializer, indexSerializer, indexSerializerWithId, binEncoder)
      case VisibilityLevel.Attribute =>
        val serializer = KryoFeatureSerializer(sft, SerializationOptions.withoutId)
        (sf) => new AccumuloAttributeLevelFeature(sf, sft, defaultVisibility, serializer, serializerWithId,
          idSerializer, indexSerializer, indexSerializerWithId, binEncoder)
    }
  }

  class RowValue(val cf: Text, val cq: Text, val vis: ColumnVisibility, toValue: => Value) {
    lazy val value: Value = toValue
  }

  class AccumuloFeatureLevelFeature(val feature: SimpleFeature,
                                    defaultVisibility: String,
                                    serializers: Seq[(Text, SimpleFeatureSerializer)],
                                    serializerWithId: SimpleFeatureSerializer,
                                    idSerializer: (String) => Array[Byte],
                                    indexSerializer: SimpleFeatureSerializer,
                                    indexSerializerWithId: SimpleFeatureSerializer,
                                    binEncoder: Option[BinEncoder]) extends AccumuloFeature {

    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

    private lazy val visibility =
      new ColumnVisibility(feature.userData[String](FEATURE_VISIBILITY).getOrElse(defaultVisibility))

    override lazy val fullValues: Seq[RowValue] = serializers.map { case (colFamily, serializer) =>
      new RowValue(colFamily, EmptyColumnQualifier, visibility, new Value(serializer.serialize(feature)))
    }

    override lazy val fullValuesWithId: Seq[RowValue] =
      Seq(new RowValue(AccumuloColumnGroups.default, EmptyColumnQualifier, visibility, new Value(serializerWithId.serialize(feature))))

    override lazy val indexValues: Seq[RowValue] =
      Seq(new RowValue(IndexColumnFamily, EmptyColumnQualifier, visibility, new Value(indexSerializer.serialize(feature))))

    override lazy val indexValuesWithId: Seq[RowValue] =
      Seq(new RowValue(IndexColumnFamily, EmptyColumnQualifier, visibility, new Value(indexSerializerWithId.serialize(feature))))

    override lazy val binValues: Seq[RowValue] = binEncoder.toSeq.map { encoder =>
      new RowValue(BinColumnFamily, EmptyColumnQualifier, visibility, new Value(encoder.encode(feature)))
    }

    override lazy val idBytes: Array[Byte] = idSerializer.apply(feature.getID)
  }

  class AccumuloAttributeLevelFeature(val feature: SimpleFeature,
                                      sft: SimpleFeatureType,
                                      defaultVisibility: String,
                                      serializer: SimpleFeatureSerializer,
                                      serializerWithId: SimpleFeatureSerializer,
                                      idSerializer: (String) => Array[Byte],
                                      indexSerializer: SimpleFeatureSerializer,
                                      indexSerializerWithId: SimpleFeatureSerializer,
                                      binEncoder: Option[BinEncoder]) extends AccumuloFeature {

    private lazy val visibilities: Array[String] = {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
      val count = feature.getFeatureType.getAttributeCount
      val userData = feature.userData[String](FEATURE_VISIBILITY)
      val visibilities = userData.map(_.split(",")).getOrElse(Array.fill(count)(defaultVisibility))
      require(visibilities.length == count,
        s"Per-attribute visibilities do not match feature type ($count values expected): ${userData.getOrElse("")}")
      visibilities
    }

    private lazy val indexGroups: Seq[(ColumnVisibility, Array[Byte])] =
      visibilities.zipWithIndex.groupBy(_._1).map { case (vis, indices) =>
        (new ColumnVisibility(vis), indices.map(_._2.toByte).sorted)
      }.toSeq

    override lazy val fullValues: Seq[RowValue] = indexGroups.map { case (vis, indices) =>
      val sf = new ScalaSimpleFeature(sft, "")
      indices.foreach(i => sf.setAttribute(i, feature.getAttribute(i)))
      new RowValue(AttributeColumnFamily, new Text(indices), vis, new Value(serializer.serialize(sf)))
    }

    override lazy val fullValuesWithId: Seq[RowValue] = indexGroups.map { case (vis, indices) =>
      val sf = new ScalaSimpleFeature(sft, "")
      indices.foreach(i => sf.setAttribute(i, feature.getAttribute(i)))
      new RowValue(AttributeColumnFamily, new Text(indices), vis, new Value(serializerWithId.serialize(sf)))
    }

    override lazy val indexValues: Seq[RowValue] = indexGroups.map { case (vis, indices) =>
      val sf = new ScalaSimpleFeature(sft, "")
      indices.foreach(i => sf.setAttribute(i, feature.getAttribute(i)))
      new RowValue(AttributeColumnFamily, new Text(indices), vis, new Value(indexSerializer.serialize(sf)))
    }

    override lazy val indexValuesWithId: Seq[RowValue] = indexGroups.map { case (vis, indices) =>
      val sf = new ScalaSimpleFeature(sft, "")
      indices.foreach(i => sf.setAttribute(i, feature.getAttribute(i)))
      new RowValue(AttributeColumnFamily, new Text(indices), vis, new Value(indexSerializerWithId.serialize(sf)))
    }

    override lazy val binValues: Seq[RowValue] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val rowOpt = for {
        encoder <- binEncoder
        trackId <- sft.getBinTrackId
        trackIndex = sft.indexOf(trackId)
        if trackIndex != -1
      } yield {
        // merge the visibilities for the individual fields
        val dateVis = sft.getDtgIndex.map(visibilities.apply)
        val geomVis = visibilities(sft.getGeomIndex)
        val trackVis = visibilities(trackIndex)
        val vis = (Seq(geomVis, trackVis) ++ dateVis).flatMap(_.split("&")).distinct.mkString("&")
        new RowValue(BinColumnFamily, EmptyColumnQualifier, new ColumnVisibility(vis), new Value(encoder.encode(feature)))
      }
      rowOpt.toSeq
    }

    override lazy val idBytes: Array[Byte] = idSerializer.apply(feature.getID)
  }
}
