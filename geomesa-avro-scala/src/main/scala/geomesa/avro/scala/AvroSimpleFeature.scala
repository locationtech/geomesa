package geomesa.avro.scala

import com.google.common.cache.{CacheLoader, LoadingCache, CacheBuilder}
import com.vividsolutions.jts.geom.Geometry
import java.io.OutputStream
import java.lang._
import java.nio._
import java.util
import java.util.concurrent.TimeUnit
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord}
import org.apache.avro.io.{DatumWriter, EncoderFactory, BinaryEncoder}
import org.apache.avro.{SchemaBuilder, Schema}
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.opengis.feature.{Property, GeometryAttribute}
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import org.opengis.filter.identity.FeatureId
import java.util.{Map => JMap, HashMap => JHashMap, Date, UUID, List => JList}
import org.opengis.geometry.BoundingBox
import org.opengis.feature.`type`.Name
import org.opengis.feature.`type`.GeometryDescriptor
import org.opengis.feature.`type`.AttributeDescriptor


class AvroSimpleFeature(id: FeatureId, sft: SimpleFeatureType) extends SimpleFeature {

  //case class Record(clazz: Class[_], name: String, idx: Int, schema: Schema)

  // Initialize class variables
  val values = new Array[Object](sft.getAttributeCount)
  val userData = new JHashMap[Object, Object]()
  val typeMap = Caches.typeMapCache.get(sft)
  val names = Caches.nameCache.get(sft)
  val nameIndex = Caches.nameIndexCache.get(sft)
  val schema = Caches.avroSchemaCache.get(sft)

  private val primitiveTypes = List(classOf[String], classOf[Integer], classOf[Long], classOf[Double],
    classOf[Float], classOf[Boolean])

  def write(os: OutputStream) {
    val encoder = EncoderFactory.get.binaryEncoder(os, null)
    val record = new GenericData.Record(schema)
    record.put(Caches.AVRO_SIMPLE_FEATURE_VERSION, Caches.VERSION)
    record.put(Caches.FEATURE_ID_AVRO_FIELD_NAME, getID)

    values.zipWithIndex.foreach { case (v, idx) =>
      val x = typeMap.get(names(idx)) match {
        case t if primitiveTypes.contains(t) => v
        case t if classOf[UUID].isAssignableFrom(t) => {
          val uuid: UUID = v.asInstanceOf[UUID]
          val bb: ByteBuffer = ByteBuffer.allocate(16)
          bb.putLong(uuid.getMostSignificantBits)
          bb.putLong(uuid.getLeastSignificantBits)
          bb.flip
          bb
        }
        case t if classOf[Date].isAssignableFrom(t) => v.asInstanceOf[Date].getTime
        case t if classOf[Geometry].isAssignableFrom(t) => v.asInstanceOf[Geometry].toText
        case _ => {
          var txt: String = Converters.convert(v, classOf[String])
          if (txt == null) {
            txt = v.toString
          }
          txt
        }
      }
      record.put(names(idx),x)
    }
    val datumWriter = new GenericDatumWriter[GenericRecord](this.schema)
    datumWriter.write(record, encoder)
    encoder.flush
  }

  def getFeatureType = sft

  def getType = sft

  def setID(id: String) = (this.id.asInstanceOf[FeatureIdImpl]).setID(id)

  def getIdentifier = id

  def getID = id.getID

  def getAttribute(name: String): Object = getAttribute(nameIndex.get(name))

  def getAttribute(name: Name): Object = getAttribute(name.getLocalPart)

  def getAttribute(index: Int): Object = values(index)

  def setAttribute(name: String, value: Object) = setAttribute(nameIndex.get(name), value)

  def setAttribute(name: Name, value: Object) = setAttribute(name.getLocalPart, value)

  def setAttribute(index: Int, value: Object) = {
    values(index) = value
  }

  def setAttributes(values: JList[Object]) = {
    import scala.collection.JavaConversions._
    values.zipWithIndex.foreach { case (v, idx) => {setAttribute(idx, v)}}
  }


  def getAttributeCount = values.length

  def getAttributes: JList[Object] = {
    import scala.collection.JavaConversions._
    return this.values.toList
  }

  def getDefaultGeometry: Object = {
    val defaultGeometry: GeometryDescriptor = sft.getGeometryDescriptor
    return if (defaultGeometry != null) getAttribute(defaultGeometry.getName) else null
  }

  def setAttributes(`object`: Array[Object]) {
    throw new UnsupportedOperationException
  }

  def setDefaultGeometry(defaultGeometry: Object) {
    val descriptor: GeometryDescriptor = sft.getGeometryDescriptor
    setAttribute(descriptor.getName, defaultGeometry)
  }

  def getBounds: BoundingBox = {
    val obj: Object = getDefaultGeometry
    if (obj.isInstanceOf[Geometry]) {
      val geometry: Geometry = obj.asInstanceOf[Geometry]
      return new ReferencedEnvelope(geometry.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    }
    return new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  def getDefaultGeometryProperty: GeometryAttribute = {
    throw new UnsupportedOperationException
  }

  def setDefaultGeometryProperty(defaultGeometry: GeometryAttribute) {
    throw new UnsupportedOperationException
  }

  def getProperties: util.Collection[Property] = {
    throw new UnsupportedOperationException
  }

  def getProperties(name: Name): util.Collection[Property] = {
    throw new UnsupportedOperationException
  }

  def getProperties(name: String): util.Collection[Property] = {
    throw new UnsupportedOperationException
  }

  def getProperty(name: Name): Property = {
    throw new UnsupportedOperationException
  }

  def getProperty(name: String): Property = {
    throw new UnsupportedOperationException
  }

  def getValue: util.Collection[_ <: Property] = {
    throw new UnsupportedOperationException
  }

  def setValue(value: util.Collection[Property]) {
    throw new UnsupportedOperationException
  }

  def getDescriptor: AttributeDescriptor = {
    throw new UnsupportedOperationException
  }

  def getName: Name = {
    throw new UnsupportedOperationException
  }

  def getUserData: JMap[Object, Object] = userData

//  def isNillable: Boolean = {
//    throw new UnsupportedOperationException
//  }

  def isNillable: scala.Boolean = true

  def setValue(value: Object) {
    throw new UnsupportedOperationException
  }

  def validate { }
}

object Caches {
  val typeMapCache: LoadingCache[SimpleFeatureType, JMap[String, Class[_]]] =
    CacheBuilder.newBuilder.maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
      .build(new CacheLoader[SimpleFeatureType, JMap[String, Class[_]]] {
      def load(sft: SimpleFeatureType): JMap[String, Class[_]] = {
        val map = new JHashMap[String, Class[_]]
        import scala.collection.JavaConversions._
        sft.getAttributeDescriptors.foreach( ad => {
          val name = ad.getLocalName
          val clazz = ad.getType.getBinding
          map.put(name, clazz)
        })
        map
      }
    })

  val avroSchemaCache: LoadingCache[SimpleFeatureType, Schema] =
    CacheBuilder.newBuilder.maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES).
      build(new CacheLoader[SimpleFeatureType, Schema] {
      def load(sft: SimpleFeatureType): Schema = {
        return generateSchema(sft)
      }
    })

  val nameCache: LoadingCache[SimpleFeatureType, Array[String]] =
    CacheBuilder.newBuilder.maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES).
      build(new CacheLoader[SimpleFeatureType, Array[String]] {
      def load(sft: SimpleFeatureType): Array[String] = {
        DataUtilities.attributeNames(sft)
      }
    })

  val nameIndexCache: LoadingCache[SimpleFeatureType, JMap[String, Integer]] =
    CacheBuilder.newBuilder.maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES).
      build(new CacheLoader[SimpleFeatureType, JMap[String, Integer]] {
      def load(sft: SimpleFeatureType): JMap[String, Integer] = {
        val map: JMap[String, Integer] = new JHashMap[String, Integer]
        DataUtilities.attributeNames(sft).foreach( name => {map.put(name, sft indexOf name)})
        map
      }
    })

  final val FEATURE_ID_AVRO_FIELD_NAME: String = "__fid__"
  final val AVRO_SIMPLE_FEATURE_VERSION: String = "__version__"
  final val VERSION: Int = 1
  final val AVRO_NAMESPACE: String = "org.geomesa"

  def generateSchema(sft: SimpleFeatureType): Schema = {
    var assembler: SchemaBuilder.FieldAssembler[_] =
      SchemaBuilder.record(sft.getTypeName).namespace(AVRO_NAMESPACE).fields
        .name(AVRO_SIMPLE_FEATURE_VERSION).`type`.intType.noDefault
        .name(FEATURE_ID_AVRO_FIELD_NAME).`type`.stringType.noDefault

    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.foreach( ad => {
      val name = ad.getLocalName
      assembler = ad.getType.getBinding match {
        case c if classOf[String].isAssignableFrom(c) => assembler.name(name).`type`.stringType.noDefault
        case c if classOf[Integer].isAssignableFrom(c) => assembler.name(name).`type`.intType.noDefault
        case c if classOf[Long].isAssignableFrom(c) => assembler.name(name).`type`.longType.noDefault
        case c if classOf[Double].isAssignableFrom(c) => assembler.name(name).`type`.doubleType.noDefault
        case c if classOf[Float].isAssignableFrom(c) => assembler.name(name).`type`.floatType.noDefault
        case c if classOf[Boolean].isAssignableFrom(c) => assembler.name(name).`type`.booleanType.noDefault
        case c if classOf[UUID].isAssignableFrom(c) => assembler.name(name).`type`.bytesType.noDefault
        case c if classOf[Date].isAssignableFrom(c)=> assembler.name(name).`type`.longType.noDefault
        case c if classOf[Geometry].isAssignableFrom(c) =>  assembler.name(name).`type`.stringType.noDefault
      }
    })
    assembler.endRecord.asInstanceOf[Schema]
  }

}
