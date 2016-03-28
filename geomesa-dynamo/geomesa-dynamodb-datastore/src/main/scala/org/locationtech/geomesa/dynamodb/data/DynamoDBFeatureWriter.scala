/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.document.{Item, PrimaryKey, Table}
import com.google.common.primitives.{Bytes, Ints, Longs}
import com.vividsolutions.jts.geom.Point
import org.geotools.data.simple.SimpleFeatureWriter
import org.joda.time.DateTime
import org.locationtech.geomesa.dynamo.core.DynamoPrimaryKey
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

trait DynamoDBPutter {
  // Allows impl changes between appending and updating writers
  protected def dynamoDBPut(t: Table, i: Item): Unit = ???
}

trait DynamoDBFeatureWriter extends SimpleFeatureWriter with DynamoDBPutter {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

  val dtgIndex = sft.getDtgIndex.get
  private[this] val attributeDescriptors = sft.getAttributeDescriptors.toList
  private[this] val encoder = new KryoFeatureSerializer(sft)
  private[this] var curFeature: SimpleFeature = null

  def sft: SimpleFeatureType

  def table: Table

  override def hasNext: Boolean = true

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)
    curFeature
  }

  override def remove(): Unit = throw new NotImplementedError("DynamoDB feature writer is append only")

  override def close(): Unit = {}

  override def getFeatureType: SimpleFeatureType = sft

  override def write(): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    val geom = curFeature.point
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])

    // hash key
    val pk = DynamoPrimaryKey(dtg, x, y)
    val hash = Ints.toByteArray(pk.idx)

    // range key
    val secondsInWeek = DynamoPrimaryKey.secondsInCurrentWeek(dtg)
    val z3 = DynamoPrimaryKey.SFC3D.index(x, y, secondsInWeek)
    val z3idx = Longs.toByteArray(z3.z)

    val range = Bytes.concat(z3idx, curFeature.getID.getBytes(StandardCharsets.UTF_8))

    val primaryKey = new PrimaryKey(
      DynamoDBDataStore.geomesaKeyHash, hash,
      DynamoDBDataStore.geomesaKeyRange, range
    )

    val item = new Item().withPrimaryKey(primaryKey)

    val attributes = curFeature.getAttributes.iterator()
    val descriptors = attributeDescriptors.iterator
    while (attributes.hasNext && descriptors.hasNext) {
      serialize(item, attributes.next(), descriptors.next())
    }

    item.withBinary(DynamoDBDataStore.serId, encoder.serialize(curFeature))

    this.dynamoDBPut(table, item)
    curFeature = null
  }

  private def serialize(item: Item, attr: AnyRef, desc: AttributeDescriptor) = {
    import java.{lang => jl}
    desc.getType.getBinding match {
      case c if c.equals(classOf[jl.Boolean]) =>
        item.withBoolean(desc.getLocalName, attr.asInstanceOf[jl.Boolean])

      case c if c.equals(classOf[jl.Integer]) =>
        item.withInt(desc.getLocalName, attr.asInstanceOf[jl.Integer])

      case c if c.equals(classOf[jl.Long]) =>
        item.withLong(desc.getLocalName, attr.asInstanceOf[jl.Long])

      case c if c.equals(classOf[jl.Float]) =>
        item.withFloat(desc.getLocalName, attr.asInstanceOf[jl.Float])

      case c if c.equals(classOf[jl.Double]) =>
        item.withDouble(desc.getLocalName, attr.asInstanceOf[jl.Double])

      case c if c.equals(classOf[String]) =>
        item.withString(desc.getLocalName, attr.asInstanceOf[String])

      case c if c.equals(classOf[UUID]) =>
        item.withBinary(desc.getLocalName, encodeUUID(attr.asInstanceOf[UUID]))

      case c if c.equals(classOf[java.util.Date]) =>
        item.withLong(desc.getLocalName, attr.asInstanceOf[java.util.Date].getTime)

      case c if classOf[Point].isAssignableFrom(c) =>
        item.withBinary(desc.getLocalName, WKBUtils.write(attr.asInstanceOf[Point]))

      case _ =>
        throw new Exception(s"Could not serialize feature attribute: ${desc.getLocalName} " +
          s"of type: ${desc.getType.getName.toString}")
    }
  }

  private def encodeUUID(uuid: UUID): ByteBuffer = {
    ByteBuffer.allocate(16)
      .putLong(uuid.getMostSignificantBits)
      .putLong(uuid.getLeastSignificantBits)
      .flip.asInstanceOf[ByteBuffer]
  }

}

class DynamoDBAppendingFeatureWriter(val sft: SimpleFeatureType, val table: Table)
  extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false

  override def dynamoDBPut(t: Table, i: Item): Unit = {
    val ps = new PutItemSpec()
      .withItem(i)
      .withConditionExpression(s"attribute_not_exists(${DynamoDBDataStore.geomesaKeyHash})")
    t.putItem(ps)
  }
}

class DynamoDBUpdatingFeatureWriter(val sft: SimpleFeatureType, val table: Table)
  extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false

  override def dynamoDBPut(t: Table, i: Item): Unit = {
    t.putItem(i)
  }
}


