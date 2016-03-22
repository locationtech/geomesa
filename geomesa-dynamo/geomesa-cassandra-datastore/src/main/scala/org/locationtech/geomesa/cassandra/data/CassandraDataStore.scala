/** *********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  * ************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.math.BigInteger
import java.net.URI
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.datastax.driver.core._
import com.google.common.collect.HashBiMap
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.data.store._
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.locationtech.geomesa.dynamo.core.SchemaValidation
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.SimpleFeatureType

object CassandraDataStore {

  import scala.collection.JavaConversions._

  val typeMap = HashBiMap.create[Class[_], DataType]
  typeMap.putAll(Map(
    classOf[Integer]    -> DataType.cint(),
    classOf[Long]       -> DataType.bigint(),
    classOf[Float]      -> DataType.cfloat(),
    classOf[Double]     -> DataType.cdouble(),
    classOf[Boolean]    -> DataType.cboolean(),
    classOf[BigDecimal] -> DataType.decimal(),
    classOf[BigInteger] -> DataType.varint(),
    classOf[String]     -> DataType.text(),
    classOf[Date]       -> DataType.timestamp(),
    classOf[UUID]       -> DataType.uuid(),
    classOf[Point]      -> DataType.blob()
  ))

  def getSchema(name: Name, table: TableMetadata): SimpleFeatureType = {
    val cols = table.getColumns.filterNot {
      c => c.getName == "pkz" || c.getName == "z31" || c.getName == "fid"
    }
    val attrTypeBuilder = new AttributeTypeBuilder()
    val attributes = cols.map { c =>
      val it = typeMap.inverse().get(c.getType)
      attrTypeBuilder.binding(it).buildDescriptor(c.getName)
    }
    // TODO: allow user data to set dtg field
    val dtgAttribute = attributes.find(_.getType.getBinding.isAssignableFrom(classOf[java.util.Date])).head
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.addAll(attributes)
    sftBuilder.setName(name.getLocalPart)
    val sft = sftBuilder.buildFeatureType()
    sft.getUserData.put(RichSimpleFeatureType.DEFAULT_DATE_KEY, dtgAttribute.getLocalName)
    sft
  }

  sealed trait FieldSerializer {
    def serialize(o: java.lang.Object): java.lang.Object
    def deserialize(o: java.lang.Object): java.lang.Object
  }

  case object GeomSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = {
      val geom = o.asInstanceOf[Point]
      ByteBuffer.wrap(WKBUtils.write(geom))
    }

    override def deserialize(o: Object): AnyRef = WKBUtils.read(o.asInstanceOf[ByteBuffer].array())
  }

  case object DefaultSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = o
    override def deserialize(o: Object): AnyRef = o
  }

  object FieldSerializer {
    def apply(attrDescriptor: AttributeDescriptor): FieldSerializer = {
      if (classOf[Geometry].isAssignableFrom(attrDescriptor.getType.getBinding)) GeomSerializer
      else DefaultSerializer
    }
  }

}

class CassandraDataStore(session: Session, keyspaceMetadata: KeyspaceMetadata, ns: URI) extends
  ContentDataStore with SchemaValidation {

  import scala.collection.JavaConversions._

  override def createFeatureSource(contentEntry: ContentEntry): ContentFeatureSource =
    new CassandraFeatureStore(contentEntry)

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    validatingCreateSchema(featureType, createSchemaInternal)
  }

  def createSchemaInternal(featureType: SimpleFeatureType): Unit = {
    val cols =
      featureType.getAttributeDescriptors.map { ad =>
        s"${ad.getLocalName}  ${CassandraDataStore.typeMap(ad.getType.getBinding).getName.toString}"
      }.mkString(",")
    val colCreate = s"(pkz int, z31 bigint, fid text, $cols, PRIMARY KEY (pkz, z31, fid))"
    val stmt = s"create table ${featureType.getTypeName} $colCreate"
    session.execute(stmt)
  }

  override def createTypeNames(): util.List[Name] =
    keyspaceMetadata.getTables.map { t => new NameImpl(ns.toString, t.getName) }.toList

  override def createContentState(entry: ContentEntry): ContentState =
    new CassandraContentState(entry, session, keyspaceMetadata.getTable(entry.getTypeName))


  override def dispose(): Unit = if (session != null) session.close()
}
