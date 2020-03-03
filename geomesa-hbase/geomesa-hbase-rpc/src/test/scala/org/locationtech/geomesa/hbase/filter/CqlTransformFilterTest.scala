/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.filter

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.{SingleRowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.IndexMode
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CqlTransformFilterTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("CqlTransformFilterTest", "name:String,*geom:Point:srid=4326")

  // serialized filters are:
  //  CqlFilter[BBOX(geom, -55.0,45.0,-45.0,55.0)]
  //  TransformFilter[name=name]
  //  CqlTransformFilter[BBOX(geom, -55.0,45.0,-45.0,55.0), name=name]

  val serialized = Seq(
    "AAAAIW5hbWU6U3RyaW5nLCpnZW9tOlBvaW50OnNyaWQ9NDMyNgAAACFCQk9YKGdlb20sIC01NS4w\nLDQ1LjAsLTQ1LjAsNTUuMCn/////AAAAAzowOv////8AAAAAAAAAAA==",
    "AAAAIW5hbWU6U3RyaW5nLCpnZW9tOlBvaW50OnNyaWQ9NDMyNgAAAAAAAAAJbmFtZT1uYW1lAAAA\nC25hbWU6U3RyaW5nAAAAAzowOv////8AAAAAAAAAAA==",
    "AAAAIW5hbWU6U3RyaW5nLCpnZW9tOlBvaW50OnNyaWQ9NDMyNgAAACFCQk9YKGdlb20sIC01NS4w\nLDQ1LjAsLTQ1LjAsNTUuMCkAAAAJbmFtZT1uYW1lAAAAC25hbWU6U3RyaW5nAAAAAzowOv////8A\nAAAAAAAAAA=="
  )

  "CqlTransformFilter" should {
    "serialize / deserialize filters without an index" in {

      val featureType = SimpleFeatureTypes.createType("cqlFilter", "name:String,geom:Point:srid=4326")
      val ecqlFilter = ECQL.toFilter("BBOX(geom, -55.0,45.0,-45.0,55.0)")

      val transformFeatureType = SimpleFeatureTypes.createType("cqlFilter", "name:String")
      val tranform = Option.apply(("name=name",transformFeatureType))

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


      /**
        * //code used for generatating base64 serialized filters. For using it temporary make NullFeatureIndex object public
        *
        *
        * //  CqlFilter[BBOX(geom, -55.0,45.0,-45.0,55.0)]
        * val bytesFilter = CqlTransformFilter(featureType, NullFeatureIndex, Option.apply(ecqlFilter),Option.empty,new Hints()).toByteArray
        * val base64Filter = Base64.encodeBytes(bytesFilter)
        * println(base64Filter)
        *
        * //  TransformFilter[name=name]
        * val bytesTrasformer = CqlTransformFilter(featureType,NullFeatureIndex,Option.empty,tranform,new Hints()).toByteArray
        * val base64Transformer = Base64.encodeBytes(bytesTrasformer)
        * println(base64Transformer)
        *
        * //  CqlTransformFilter[BBOX(geom, -55.0,45.0,-45.0,55.0), name=name]
        * val bytesTransformFilter = CqlTransformFilter(featureType,NullFeatureIndex,Option.apply(ecqlFilter),tranform,new Hints()).toByteArray
        * val base64TransformerFilter = Base64.encodeBytes(bytesTransformFilter)
        * println(base64TransformerFilter)
        *
        *
        * */

      val base64Filter = serialized(0)
      val base64Transformer = serialized(1)
      val base64TransformerFilter = serialized(2)

      val filter = CqlTransformFilter.parseFrom(Base64.decode(base64Filter))
      val transform = CqlTransformFilter.parseFrom(Base64.decode(base64Transformer))
      val filterTransform = CqlTransformFilter.parseFrom(Base64.decode(base64TransformerFilter))

      def getAttributes(cell: Cell): Seq[AnyRef] =
        serializer.deserialize(cell.getValueArray, cell.getValueOffset, cell.getValueLength).getAttributes.asScala

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
