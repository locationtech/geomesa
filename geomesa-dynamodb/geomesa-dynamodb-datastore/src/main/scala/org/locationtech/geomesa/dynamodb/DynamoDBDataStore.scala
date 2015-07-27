package org.locationtech.geomesa.dynamodb

import java.util
import java.util.Date

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, KeyAttribute, Table}
import com.amazonaws.services.dynamodbv2.model._
import com.google.common.collect.Lists
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query, Transaction}
import org.geotools.feature.NameImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class DynamoDBDataStore(catalog: String, dynamoDB: DynamoDB) extends ContentDataStore {

  private val CATALOG_TABLE = catalog
  private val catalogTable = getOrCreateCatalogTable(dynamoDB, CATALOG_TABLE)

  private def getOrCreateCatalogTable(dynamoDB: DynamoDB, table: String) = {
    val tables = dynamoDB.listTables().iterator().toList
    val ret = tables
      .find(_.getTableName == table)
      .getOrElse(
        dynamoDB.createTable(
          table,
          util.Arrays.asList(new KeySchemaElement("feature", KeyType.HASH)),
          util.Arrays.asList(new AttributeDefinition("feature", ScalarAttributeType.S)),
          new ProvisionedThroughput()
            .withReadCapacityUnits(5L)
            .withWriteCapacityUnits(6L)))
    ret.waitForActive()
    ret
  }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    import java.{lang => jl}
    val name = featureType.getTypeName

    val attrDefs =
      featureType.getAttributeDescriptors.map { attr =>
        attr.getType.getBinding match {
          case c if c.equals(classOf[String]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.S)

          case c if c.equals(classOf[jl.Integer]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if c.equals(classOf[jl.Double]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if c.equals(classOf[java.util.Date]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(c) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.B)
        }
      }

    val keySchema =
      List(
        new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH),
        new KeySchemaElement().withAttributeName("z3").withKeyType(KeyType.RANGE)
      )

    val tableDesc =
      new CreateTableRequest()
        .withTableName(s"${catalog}_${name}_z3")
        .withKeySchema(keySchema)
        .withAttributeDefinitions(
          Lists.newArrayList(
            new AttributeDefinition("id", ScalarAttributeType.S),
            new AttributeDefinition("z3", ScalarAttributeType.N)
          )
        )
        .withProvisionedThroughput(new ProvisionedThroughput(5L, 6L))

    // create the z3 index
    val res = dynamoDB.createTable(tableDesc)

    // write the meta-data
    val metaEntry = new Item().withPrimaryKey("feature", name).withString("sft", SimpleFeatureTypes.encodeType(featureType))
    catalogTable.putItem(metaEntry)

    res.waitForActive()
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val sft =
      Option(entry.getState(Transaction.AUTO_COMMIT).getFeatureType).getOrElse { getSchema(entry) }
    val table = dynamoDB.getTable(s"${catalog}_${sft.getTypeName}_z3")
    new DynamoDBFeatureSource(entry, sft, table)
  }

  def getSchema(entry: ContentEntry) = {
    val item = catalogTable.getItem("feature", entry.getTypeName)
    SimpleFeatureTypes.createType(entry.getTypeName, item.getString("sft"))
  }

  override def createTypeNames(): util.List[Name] = {
    // read types from catalog
    catalogTable.scan().iterator().map { i => new NameImpl(i.getString("feature")) }.toList
  }

}

class DynamoDBFeatureSource(entry: ContentEntry,
                            sft: SimpleFeatureType,
                            table: Table)
  extends ContentFeatureStore(entry, Query.ALL) {

  override def buildFeatureType(): SimpleFeatureType = sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = ???

  override def getCountInternal(query: Query): Int = ???

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = ???

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    new DynamoDBFeatureWriter(sft, table)
}

class DynamoDBFeatureWriter(sft: SimpleFeatureType, table: Table) extends SimpleFeatureWriter {
  private val SFC = new Z3SFC
  private var curFeature: SimpleFeature = null
  private val dtgIndex =
    sft.getAttributeDescriptors
      .zipWithIndex
      .find { case (ad, idx) => classOf[java.util.Date].equals(ad.getType.getBinding) }
      .map  { case (_, idx)  => idx }
      .getOrElse(throw new RuntimeException("No date attribute"))

  private val encoder = new KryoFeatureSerializer(sft)

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature("", sft)
    curFeature
  }

  override def remove(): Unit = ???

  override def hasNext: Boolean = true

  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  override def write(): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    // write
    val geom = curFeature.point
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])
    val weeks = epochWeeks(dtg)

    val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
    val z3 = SFC.index(x, y, secondsInWeek)

    val id = curFeature.getID
    val item = new Item().withKeyComponents(new KeyAttribute("id", id), new KeyAttribute("z3", z3.z))

    curFeature.getAttributes.zip(sft.getAttributeDescriptors).foreach { case (attr, desc) =>
      import java.{lang => jl}
      desc.getType.getBinding match {
        case c if c.equals(classOf[String]) =>
          item.withString(desc.getLocalName, attr.asInstanceOf[String])

        case c if c.equals(classOf[jl.Integer]) =>
          item.withInt(desc.getLocalName, attr.asInstanceOf[jl.Integer])

        case c if c.equals(classOf[jl.Double]) =>
          item.withDouble(desc.getLocalName, attr.asInstanceOf[jl.Double])

        case c if c.equals(classOf[java.util.Date]) =>
          item.withLong(desc.getLocalName, attr.asInstanceOf[java.util.Date].getTime)

        case c if classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(c) =>
          item.withBinary(desc.getLocalName, WKBUtils.write(attr.asInstanceOf[Geometry]))
      }
    }

    item.withBinary("ser", encoder.serialize(curFeature))
    table.putItem(item)
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}

}