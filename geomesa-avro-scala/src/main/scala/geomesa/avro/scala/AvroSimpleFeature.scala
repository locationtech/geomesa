package geomesa.avro.scala


import collection.JavaConversions._
import com.google.common.cache.{CacheLoader, LoadingCache, CacheBuilder}
import com.vividsolutions.jts.geom.Geometry
import java.io.OutputStream
import java.lang._
import java.nio._
import java.util
import java.util.concurrent.TimeUnit
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.{SchemaBuilder, Schema}
import org.geotools.data.DataUtilities
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.opengis.feature.{Property, GeometryAttribute}
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import org.opengis.filter.identity.FeatureId
import java.util.{Map => JMap, Date, UUID, List => JList}
import org.opengis.geometry.BoundingBox
import org.opengis.feature.`type`.Name
import org.opengis.feature.`type`.AttributeDescriptor
import scala.util.Try


class AvroSimpleFeature(id: FeatureId, sft: SimpleFeatureType) extends SimpleFeature {

  import AvroSimpleFeature._

  val values    = Array[AnyRef].ofDim(sft.getAttributeCount)
  val userData  = collection.mutable.HashMap.empty[AnyRef, AnyRef]
  val typeMap   = typeMapCache.get(sft)
  val names     = nameCache.get(sft)
  val nameIndex = nameIndexCache.get(sft)
  val schema    = avroSchemaCache.get(sft)

  def write(os: OutputStream) {
    val encoder = EncoderFactory.get.binaryEncoder(os, null)
    val record = new GenericData.Record(schema)
    record.put(Caches.AVRO_SIMPLE_FEATURE_VERSION, Caches.VERSION)
    record.put(Caches.FEATURE_ID_AVRO_FIELD_NAME, getID)

    values.zipWithIndex.foreach { case (v, idx) =>
      val x = typeMap.get(names(idx)) match {
        case t if primitiveTypes.contains(t) =>
          v

        case t if classOf[UUID].isAssignableFrom(t) =>
          val uuid = v.asInstanceOf[UUID]
          val bb = ByteBuffer.allocate(16)
          bb.putLong(uuid.getMostSignificantBits)
          bb.putLong(uuid.getLeastSignificantBits)
          bb.flip
          bb

        case t if classOf[Date].isAssignableFrom(t) =>
          v.asInstanceOf[Date].getTime

        case t if classOf[Geometry].isAssignableFrom(t) =>
          v.asInstanceOf[Geometry].toText

        case _ =>
          Option(Converters.convert(v, classOf[String])).getOrElse(_.toString)
      }

      record.put(names(idx), x)
    }
    val datumWriter = new GenericDatumWriter[GenericRecord](this.schema)
    datumWriter.write(record, encoder)
    encoder.flush()
  }

  def getFeatureType = sft
  def getType = sft
  def setID(idStr: String) = id.setID(id)
  def getIdentifier = id
  def getID = id.getID
  def getAttribute(name: String) = getAttribute(nameIndex.get(name))
  def getAttribute(name: Name) = getAttribute(name.getLocalPart)
  def getAttribute(index: Int) = values(index)
  def setAttribute(name: String, value: Object) = setAttribute(nameIndex.get(name), value)
  def setAttribute(name: Name, value: Object) = setAttribute(name.getLocalPart, value)
  def setAttribute(index: Int, value: Object) = values(index) = value
  def setAttributes(values: JList[Object]) =
    values.zipWithIndex.foreach { case (v, idx) => {setAttribute(idx, v)}}

  def getAttributeCount = values.length
  def getAttributes: JList[Object] = values.toList
  def getDefaultGeometry: Object =
    Try(sft.getGeometryDescriptor.getName).map { getAttribute(_) }.getOrElse(null)

  def setAttributes(`object`: Array[Object])= ???
  def setDefaultGeometry(defaultGeometry: Object) =
    setAttribute(sft.getGeometryDescriptor.getName, defaultGeometry)

  def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry =>
      new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ =>
      new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  def getDefaultGeometryProperty: GeometryAttribute = ???
  def setDefaultGeometryProperty(defaultGeometry: GeometryAttribute) = ???
  def getProperties: util.Collection[Property] = ???
  def getProperties(name: Name): util.Collection[Property] = ???
  def getProperties(name: String): util.Collection[Property] = ???
  def getProperty(name: Name): Property = ???
  def getProperty(name: String): Property = ???
  def getValue: util.Collection[_ <: Property] = ???
  def setValue(value: util.Collection[Property]) = ???
  def getDescriptor: AttributeDescriptor = ???
  def getName: Name = ???
  def getUserData = userData
  def isNillable = true
  def setValue(value: Object) = ???
  def validate() = { }
}

object AvroSimpleFeature {
  import scala.collection.JavaConversions._

  val primitiveTypes =
    List(
      classOf[String],
      classOf[Integer],
      classOf[Long],
      classOf[Double],
      classOf[Float],
      classOf[Boolean]
    )

  val typeMapCache: LoadingCache[SimpleFeatureType, Map[String, Class[_]]] =
    CacheBuilder
      .newBuilder
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(
        new CacheLoader[SimpleFeatureType, Map[String, Class[_]]] {
          def load(sft: SimpleFeatureType): Map[String, Class[_]] =
            sft.getAttributeDescriptors.map { ad =>
              val name = ad.getLocalName
              val clazz = ad.getType.getBinding
              (name, clazz)
            }.toMap
        }
      )

  val avroSchemaCache: LoadingCache[SimpleFeatureType, Schema] =
    CacheBuilder
      .newBuilder
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES).
      build(
        new CacheLoader[SimpleFeatureType, Schema] {
          def load(sft: SimpleFeatureType): Schema = generateSchema(sft)
        }
      )

  val nameCache: LoadingCache[SimpleFeatureType, Array[String]] =
    CacheBuilder
      .newBuilder
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES).
      build(
        new CacheLoader[SimpleFeatureType, Array[String]] {
          def load(sft: SimpleFeatureType) = DataUtilities.attributeNames(sft)
        }
      )

  val nameIndexCache: LoadingCache[SimpleFeatureType, Map[String, Integer]] =
    CacheBuilder
      .newBuilder
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES).
      build(
        new CacheLoader[SimpleFeatureType, Map[String, Int]] {
          def load(sft: SimpleFeatureType): JMap[String, Integer] =
            DataUtilities.attributeNames(sft).map { name => (name, sft.indexOf(name)) }.toMap
        }
      )

  final val FEATURE_ID_AVRO_FIELD_NAME: String = "__fid__"
  final val AVRO_SIMPLE_FEATURE_VERSION: String = "__version__"
  final val VERSION: Int = 1
  final val AVRO_NAMESPACE: String = "org.geomesa"

  def generateSchema(sft: SimpleFeatureType): Schema = {
    var assembler: SchemaBuilder.FieldAssembler[_] =
      SchemaBuilder.record(sft.getTypeName)
        .namespace(AVRO_NAMESPACE).fields
        .name(AVRO_SIMPLE_FEATURE_VERSION).`type`.intType.noDefault
        .name(FEATURE_ID_AVRO_FIELD_NAME).`type`.stringType.noDefault

    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.foreach { ad =>
      val name = ad.getLocalName
      assembler = ad.getType.getBinding match {
        case c if classOf[String].isAssignableFrom(c)   => assembler.name(name).`type`.stringType.noDefault
        case c if classOf[Integer].isAssignableFrom(c)  => assembler.name(name).`type`.intType.noDefault
        case c if classOf[Long].isAssignableFrom(c)     => assembler.name(name).`type`.longType.noDefault
        case c if classOf[Double].isAssignableFrom(c)   => assembler.name(name).`type`.doubleType.noDefault
        case c if classOf[Float].isAssignableFrom(c)    => assembler.name(name).`type`.floatType.noDefault
        case c if classOf[Boolean].isAssignableFrom(c)  => assembler.name(name).`type`.booleanType.noDefault
        case c if classOf[UUID].isAssignableFrom(c)     => assembler.name(name).`type`.bytesType.noDefault
        case c if classOf[Date].isAssignableFrom(c)     => assembler.name(name).`type`.longType.noDefault
        case c if classOf[Geometry].isAssignableFrom(c) => assembler.name(name).`type`.stringType.noDefault
      }
    }
    assembler.endRecord.asInstanceOf[Schema]
  }

}
