package geomesa.avro.scala

import java.util.{Date, UUID}
import org.apache.avro.io.{Decoder, DatumReader}
import org.apache.avro.Schema
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.util.Converters
import org.opengis.feature.simple.SimpleFeatureType
import scala.collection.immutable.HashSet
import com.vividsolutions.jts.geom.Geometry


class FeatureSpecificReader(oldType: SimpleFeatureType, newType: SimpleFeatureType) extends  DatumReader[AvroSimpleFeature]{

  var oldSchema = Caches.generateSchema(oldType)
  val newSchema = Caches.generateSchema(newType)
  val fieldsDesired = new HashSet() ++ DataUtilities.attributeNames(newType)

  def isDataField(f: Schema.Field):Boolean =
    !List(Caches.FEATURE_ID_AVRO_FIELD_NAME, Caches.AVRO_SIMPLE_FEATURE_VERSION).contains(f.name())

  import scala.collection.JavaConversions._
  val dataFields = oldSchema.getFields().filter(isDataField _)

  val typeMap = new collection.mutable.HashMap[String, Class[_]]
  oldType.getAttributeDescriptors.foreach(ad => {
    typeMap(ad.getLocalName) = ad.getType.getBinding
  })

  def setSchema(schema:Schema) = {this.oldSchema = schema}

  def read(reuse: AvroSimpleFeature, in: Decoder) : AvroSimpleFeature = {
    // Read the version first
    in.readInt()

    // Read the id
    val id = new FeatureIdImpl(in.readString())

    // Followed by the data fields
    val sf = new AvroSimpleFeature(id,newType)
    if(dataFields.size != fieldsDesired.size) {
      dataFields.foreach( f => setOrConsume(sf, f.name, in, typeMap.get(f.name).get))
    }
    else {
      dataFields.foreach(f => set(sf, f.name, in, typeMap.get(f.name).get))
    }
    sf
  }

  protected def setOrConsume(sf: AvroSimpleFeature, field: String, in:Decoder, cls: Class[_]) = {
    if (fieldsDesired.contains(field)) {
      set(sf,field, in, cls)
    }
    else{
      consume(cls, in)
    }
  }

  protected def set(sf: AvroSimpleFeature, field: String, in:Decoder, cls: Class[_]) = {
    val obj: Object = cls match {
      case c if classOf[String].isAssignableFrom(cls) => in.readString().toString
      case c if classOf[java.lang.Integer].isAssignableFrom(cls) => in.readInt().asInstanceOf[Object]
      case c if classOf[java.lang.Long].isAssignableFrom(cls) => in.readLong().asInstanceOf[Object]
      case c if classOf[java.lang.Double].isAssignableFrom(cls) => in.readDouble().asInstanceOf[Object]
      case c if classOf[java.lang.Float].isAssignableFrom(cls) => in.readFloat().asInstanceOf[Object]
      case c if classOf[java.lang.Boolean].isAssignableFrom(cls) => in.readBoolean().asInstanceOf[Object]
      case c if classOf[UUID].isAssignableFrom(cls) => {
        val bb = in.readBytes(null)
        new UUID(bb.getLong, bb.getLong)
      }
      case c if classOf[Date].isAssignableFrom(cls) => new Date(in.readLong())
      case c if classOf[Geometry].isAssignableFrom(cls) => Converters.convert(in.readString(), cls).asInstanceOf[Object]
    }
    sf.setAttribute(field, obj)
  }

  protected def consume(cls: Class[_], in:Decoder) = cls match {
    case c if classOf[java.lang.String].isAssignableFrom(cls) => in.skipString()
    case c if classOf[java.lang.Integer].isAssignableFrom(cls) => in.readInt()
    case c if classOf[java.lang.Long].isAssignableFrom(cls) => in.readLong()
    case c if classOf[java.lang.Double].isAssignableFrom(cls) => in.readDouble()
    case c if classOf[java.lang.Float].isAssignableFrom(cls) => in.readFloat()
    case c if classOf[java.lang.Boolean].isAssignableFrom(cls) => in.readBoolean()
    case c if classOf[UUID].isAssignableFrom(cls) => in.skipBytes()
    case c if classOf[Date].isAssignableFrom(cls) => in.readLong()
    case c if classOf[Geometry].isAssignableFrom(cls) => in.skipString()
  }
}

object FeatureSpecificReader{
  def apply(sftType: SimpleFeatureType) = new FeatureSpecificReader(sftType, sftType)
}
