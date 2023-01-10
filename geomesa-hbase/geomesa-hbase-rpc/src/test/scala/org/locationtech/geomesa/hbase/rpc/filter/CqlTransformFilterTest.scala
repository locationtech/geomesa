/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.filter

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.{SingleRowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Base64

@RunWith(classOf[JUnitRunner])
class CqlTransformFilterTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("CqlTransformFilterTest", "name:String,*geom:Point:srid=4326")

  // serialized filters are:
  //  CqlFilter[BBOX(geom, -55.0,45.0,-45.0,55.0)]
  //  TransformFilter[name=name]
  //  CqlTransformFilter[BBOX(geom, -55.0,45.0,-45.0,55.0), name=name]

  val serialized = Seq(
    "AAAAIW5hbWU6U3RyaW5nLCpnZW9tOlBvaW50OnNyaWQ9NDMyNgAAACFCQk9YKGdlb20sIC01NS4wLDQ1LjAsLTQ1LjAsNTUuMCn/////AAAAAzowOv////8AAAAAAAAAAA==",
    "AAAAIW5hbWU6U3RyaW5nLCpnZW9tOlBvaW50OnNyaWQ9NDMyNgAAAAAAAAAJbmFtZT1uYW1lAAAAC25hbWU6U3RyaW5nAAAAAzowOv////8AAAAAAAAAAA==",
    "AAAAIW5hbWU6U3RyaW5nLCpnZW9tOlBvaW50OnNyaWQ9NDMyNgAAACFCQk9YKGdlb20sIC01NS4wLDQ1LjAsLTQ1LjAsNTUuMCkAAAAJbmFtZT1uYW1lAAAAC25hbWU6U3RyaW5nAAAAAzowOv////8AAAAAAAAAAA=="
  )

  "CqlTransformFilter" should {
    "serialize / deserialize filters without an index" in {
      val tsft = SimpleFeatureTypes.createType("", "name:String")
      val serializer = KryoFeatureSerializer.builder(tsft).withoutId.build()
      val wrapper = WritableFeature.wrapper(sft, new ColumnGroups())
      val index = new Z2Index(null, sft, "geom", IndexMode.ReadWrite)
      val converter = index.createConverter()
      val features = Seq(
        ScalaSimpleFeature.create(sft, "0", "name0", "POINT (-50 50)"),
        ScalaSimpleFeature.create(sft, "1", "name1", "POINT (-60 60)")
      )
      val kvs = features.map(f => converter.convert(wrapper.wrap(f)).asInstanceOf[SingleRowKeyValue[Long]])
      val cells = kvs.map(kv => new KeyValue(kv.row, Array.empty[Byte], Array.empty[Byte], kv.values.head.value))

      val base64Filter = serialized(0)
      val base64Transformer = serialized(1)
      val base64TransformerFilter = serialized(2)

      val filter = CqlTransformFilter.parseFrom(Base64.getDecoder.decode(base64Filter))
      val transform = CqlTransformFilter.parseFrom(Base64.getDecoder.decode(base64Transformer))
      val filterTransform = CqlTransformFilter.parseFrom(Base64.getDecoder.decode(base64TransformerFilter))

      def getAttributes(cell: Cell): Seq[AnyRef] =
        serializer.deserialize(cell.getValueArray, cell.getValueOffset, cell.getValueLength).getAttributes.asScala.toSeq

      filter.filterKeyValue(cells.head) mustEqual ReturnCode.INCLUDE
      filter.transformCell(cells.head) mustEqual cells.head
      filter.filterKeyValue(cells.last) mustEqual ReturnCode.SKIP

      transform.filterKeyValue(cells.head) mustEqual ReturnCode.INCLUDE
      getAttributes(transform.transformCell(cells.head)) mustEqual Seq("name0")
      transform.filterKeyValue(cells.last) mustEqual ReturnCode.INCLUDE
      getAttributes(transform.transformCell(cells.last)) mustEqual Seq("name1")

      filterTransform.filterKeyValue(cells.head) mustEqual ReturnCode.INCLUDE
      getAttributes(filterTransform.transformCell(cells.head)) mustEqual Seq("name0")
      filterTransform.filterKeyValue(cells.last) mustEqual ReturnCode.SKIP
    }
  }
}

object CqlTransformFilterTest {
  def encode(): Unit = {

    //code used for generating base64 serialized filters

    import org.geotools.util.factory.Hints
    import org.locationtech.geomesa.hbase.rpc.filter.CqlTransformFilter.NullFeatureIndex

    val featureType = SimpleFeatureTypes.createType("cqlFilter", "name:String,geom:Point:srid=4326")
    val ecqlFilter = ECQL.toFilter("BBOX(geom, -55.0,45.0,-45.0,55.0)")

    val transformFeatureType = SimpleFeatureTypes.createType("cqlFilter", "name:String")
    val transform = Option(("name=name", transformFeatureType))

    //  CqlFilter[BBOX(geom, -55.0,45.0,-45.0,55.0)]
    val bytesFilter = CqlTransformFilter(featureType, NullFeatureIndex, Option(ecqlFilter), None, new Hints()).toByteArray
    val base64Filter = Base64.getEncoder.encodeToString(bytesFilter)
    println(base64Filter)

    //  TransformFilter[name=name]
    val bytesTransformer = CqlTransformFilter(featureType, NullFeatureIndex, None, transform, new Hints()).toByteArray
    val base64Transformer = Base64.getEncoder.encodeToString(bytesTransformer)
    println(base64Transformer)

    //  CqlTransformFilter[BBOX(geom, -55.0,45.0,-45.0,55.0), name=name]
    val bytesTransformFilter = CqlTransformFilter(featureType, NullFeatureIndex, Option(ecqlFilter), transform, new Hints()).toByteArray
    val base64TransformerFilter = Base64.getEncoder.encodeToString(bytesTransformFilter)
    println(base64TransformerFilter)
  }
}
