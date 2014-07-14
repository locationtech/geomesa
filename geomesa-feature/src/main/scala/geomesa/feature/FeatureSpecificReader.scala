package geomesa.feature

import java.io.InputStream
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.apache.avro.Schema
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.util.Converters
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.immutable.HashSet


class FeatureSpecificReader(oldType: SimpleFeatureType, newType: SimpleFeatureType)
  extends DatumReader[AvroSimpleFeature] {

  import geomesa.feature.AvroSimpleFeature._

import scala.collection.JavaConversions._

  var oldSchema = AvroSimpleFeature.generateSchema(oldType)
  val newSchema = AvroSimpleFeature.generateSchema(newType)
  val fieldsDesired = new HashSet() ++ DataUtilities.attributeNames(newType)

  def isDataField(f: Schema.Field) =
    !f.name.equals(FEATURE_ID_AVRO_FIELD_NAME) && !f.name.equals(AVRO_SIMPLE_FEATURE_VERSION)

  val dataFields = oldSchema.getFields.filter { isDataField }

  val typeMap: Map[String, Class[_]] =
    oldType.getAttributeDescriptors.map { ad => encodeAttributeName(ad.getLocalName) -> ad.getType.getBinding }.toMap

  val nullMap: Map[String, Boolean] =
    oldType.getAttributeDescriptors.map { ad => encodeAttributeName(ad.getLocalName) -> ad.isNillable }.toMap

  def setSchema(schema:Schema) = oldSchema = schema

  def read(reuse: AvroSimpleFeature, in: Decoder): AvroSimpleFeature = {
    // Read the version first
    in.readInt()

    // Read the id
    val id = new FeatureIdImpl(in.readString())

    // Followed by the data fields
    val sf = new AvroSimpleFeature(id, newType)
    if(dataFields.size != fieldsDesired.size)
      dataFields.foreach { f =>
        if(checkNull(f.name, in)) in.readNull()
        else setOrConsume(sf, decodeAttributeName(f.name), in, typeMap.get(f.name).get)
      }
    else
      dataFields.foreach { f =>
        if(checkNull(f.name, in)) in.readNull()
        else set(sf, decodeAttributeName(f.name), in, typeMap.get(f.name).get)
      }
    sf
  }

  // null at index 1 according to schema builder...beware 1.7.5 is avro version...might change with later versions
  // look at the json schema to verify the position that the null is in
  protected def checkNull(field:String, in:Decoder) = nullMap.get(field).get && in.readIndex() == 1

  protected def setOrConsume(sf: AvroSimpleFeature, field: String, in:Decoder, cls: Class[_]) =
    if (fieldsDesired.contains(field)) set(sf,field, in, cls)
    else consume(cls, in)

  protected def set(sf: AvroSimpleFeature, field: String, in:Decoder, cls: Class[_]) = {
    val obj = cls match {
      case c if classOf[String].isAssignableFrom(cls)            => in.readString()
      case c if classOf[java.lang.Integer].isAssignableFrom(cls) => in.readInt().asInstanceOf[Object]
      case c if classOf[java.lang.Long].isAssignableFrom(cls)    => in.readLong().asInstanceOf[Object]
      case c if classOf[java.lang.Double].isAssignableFrom(cls)  => in.readDouble().asInstanceOf[Object]
      case c if classOf[java.lang.Float].isAssignableFrom(cls)   => in.readFloat().asInstanceOf[Object]
      case c if classOf[java.lang.Boolean].isAssignableFrom(cls) => in.readBoolean().asInstanceOf[Object]

      case c if classOf[UUID].isAssignableFrom(cls) =>
        val bb = in.readBytes(null)
        new UUID(bb.getLong, bb.getLong)

      case c if classOf[Date].isAssignableFrom(cls) =>
        new Date(in.readLong())

      case c if classOf[Geometry].isAssignableFrom(cls) =>
        Converters.convert(in.readString(), cls).asInstanceOf[Object]
    }
    sf.setAttributeNoConvert(field, obj)
  }

  protected def consume(cls: Class[_], in:Decoder) = cls match {
    case c if classOf[java.lang.String].isAssignableFrom(cls)  => in.skipString()
    case c if classOf[java.lang.Integer].isAssignableFrom(cls) => in.readInt()
    case c if classOf[java.lang.Long].isAssignableFrom(cls)    => in.readLong()
    case c if classOf[java.lang.Double].isAssignableFrom(cls)  => in.readDouble()
    case c if classOf[java.lang.Float].isAssignableFrom(cls)   => in.readFloat()
    case c if classOf[java.lang.Boolean].isAssignableFrom(cls) => in.readBoolean()
    case c if classOf[UUID].isAssignableFrom(cls)              => in.skipBytes()
    case c if classOf[Date].isAssignableFrom(cls)              => in.readLong()
    case c if classOf[Geometry].isAssignableFrom(cls)          => in.skipString()
  }
}

object FeatureSpecificReader {

  // use when you want the entire feature back, not a subset
  def apply(sftType: SimpleFeatureType) = new FeatureSpecificReader(sftType, sftType)

  // first field is serialization version, 2nd field is ID of simple feature
  def extractId(is: InputStream): String = {
    val decoder = DecoderFactory.get().binaryDecoder(is, null)
    decoder.readInt()
    decoder.readString()
  }
}
