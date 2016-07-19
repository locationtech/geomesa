package org.locationtech.geomesa.features.avro.complex

import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type._
import org.apache.avro.io.{Decoder, Encoder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.geotools.feature.NameImpl
import org.geotools.gml3.GMLSchema
import org.geotools.xs.XSSchema
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._
import org.opengis.feature.`type`._

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.immutable.{BitSet, Queue}
import scala.collection.mutable

object AvroSchemas {
  val FEATURE_ID_AVRO_FIELD_NAME = "__fid__"
  val AVRO_SIMPLE_FEATURE_VERSION = "__version__"
  val AVRO_SIMPLE_FEATURE_USERDATA = "__userdata__"
  val VERSION = 4
  val AVRO_NAMESPACE = "org.geomesa"
  val NAME_SIMPLE_CONTENT = new NameImpl(AVRO_NAMESPACE, "simpleContent")
  val GEOT_SIMPLE_CONTENT = new NameImpl("simpleContent")
  val USERDATAITEM_SCHEMA = SchemaBuilder.record("userDataItem")
    .namespace(AVRO_NAMESPACE)
    .fields()
    .name("class").`type`.stringType().noDefault()
    .name("key").`type`.stringType().noDefault()
    .name("value").`type`.stringType().noDefault().endRecord()

  trait FieldNameEncoder {
    def encode(name:String): String
    def decode(name:String): String

    def decode(s:Schema): Name = {
      s.getType match {
        case RECORD|ENUM|FIXED => new NameImpl(decode(s.getNamespace), decode(s.getName))
        case _ => new NameImpl(decode(s.getProp("namespace")), decode(s.getName))
      }
    }
    def decode(f:Field, parent:Schema): Name = {
      val ns = Option(f.getProp("namespace")).getOrElse(parent.getNamespace)
      new NameImpl(decode(ns), decode(f.name))
    }
  }

  object FieldNameEncoderV1 extends FieldNameEncoder{
    private val radix = "0123456789ABCDEF"
    private val allowedCharsInit:BitSet = ('A'.toInt.to('Z')++'a'.toInt.to('z')).map(identity)(scala.collection.breakOut)
    private val allowedChars:BitSet = allowedCharsInit ++ '0'.toInt.to('9')

    private def encodeCharInit(b:Byte) = if(allowedCharsInit(b)) b.toChar.toString else "_"+radix((b>>4)&0xf)+radix(b&0xf)
    private def encodeChar(b:Byte) = if(allowedChars(b)) b.toChar.toString else "_"+radix((b>>4)&0xf)+radix(b&0xf)
    private def fromHex(c:Char):Int = if(c>='0' && c<='9') c-'0' else if(c>='A' && c<='F') c-'A'+10 else if(c>='a' && c<='f') c-'a'+10 else throw new IllegalArgumentException()

    override def encode(name: String): String = if(name == null) null else name.getBytes("UTF-8") match {
      case scala.Array() => ""
      case scala.Array(x,xs@_*) => (encodeCharInit(x) +: xs.map(encodeChar)).mkString
    }

    override def decode(name: String): String = {
      @tailrec
      def dec(i:Int, accum:Queue[String]):Queue[String] = if(i >= name.length) accum else name.charAt(i) match {
        case '_'=> dec(i+3, accum:+((fromHex(name.charAt(i+1))<<4)+fromHex(name.charAt(i+2))).toChar.toString)
        case c=>
          val j = {val tmp=name.indexOf('_',i); if(tmp < 0) name.length else tmp}
          dec(j,accum:+name.substring(i, j))
      }
      if(name == null) null else dec(0, Queue.empty).mkString
    }
  }

  class SchemaFactory(nameEncoder: FieldNameEncoder,
                      registry: AvroCodecs.CodecRegistry,
                      namespace: String = AVRO_NAMESPACE) extends (ComplexType=>Schema) {
    private[this] val typeCache = mutable.Map.empty[ComplexType,Schema]

    def apply(ct: ComplexType): Schema = {
      typeCache.get(ct) match { case Some(x) => return x case _ => }
      val schema = Schema.createRecord(nameEncoder.encode(ct.getName.getLocalPart), null, nameEncoder.encode(ct.getName.getNamespaceURI), false)
      typeCache(ct)=schema

      val preludeFields = if(isDescendedFrom(ct, XSSchema.ANYSIMPLETYPE_TYPE)) {
        val st = nearestSimpleType(ct)
        for(codec<-registry.find(st.getBinding)) yield
          computeField(ct, NAME_SIMPLE_CONTENT, computeType(codec.schema, nillable = true, multi = false))
      } else {
        None
      }

      schema.setFields((preludeFields ++ computeFields(ct)).toList)
      schema
    }

    private[this] def computeField(parent:ComplexType, name:Name, s:Schema) = {
      val f = new Field(nameEncoder.encode(name.getLocalPart), s, null, null)
      if(name.getNamespaceURI != parent.getName.getNamespaceURI) {
        f.addProp("namespace", nameEncoder.encode(name.getNamespaceURI))
      }
      f
    }

    private[this] def computeType(s:Schema, ad:AttributeDescriptor):Schema =
      computeType(s, ad.getMinOccurs == 0 || ad.isNillable, ad.getMaxOccurs != 1)

    private[this] def computeType(s:Schema, nillable:Boolean, multi: Boolean):Schema = {
      val maybeMulti = if(multi) Schema.createArray(s) else s
      if(nillable) Schema.createUnion(List(Schema.create(Schema.Type.NULL), maybeMulti)) else maybeMulti
    }

    private[this] def computeFields[R](parent: ComplexType): Iterable[Field] = {
      allProps(parent).map(_.asInstanceOf[AttributeDescriptor]).flatMap { ad=>
        ad.getType match {
          case ct: ComplexType =>
            Some(computeField(parent, ad.getName, computeType(this(ct), ad)))
          case _ if ad.isList =>
            for(clazz<-ad.getListType(); codec<-registry.find(clazz)) yield
              computeField(parent, ad.getName, computeType(Schema.createArray(codec.schema), ad))
          case _ if ad.isMap =>
            for((keyClass,valClass)<-ad.getMapTypes();
                keyCodec<-registry.find(keyClass);
                valCodec<-registry.find(valClass)) yield {
              val keyField = new Field("k", keyCodec.schema, null, null)
              val valField = new Field("v", valCodec.schema, null, null)
              val schema = Schema.createRecord(List(keyField,valField))
              computeField(parent, ad.getName, computeType(Schema.createArray(schema), ad))
            }
          case _ =>
            val st = nearestSimpleType(ad.getType)
            for(codec<-registry.find(st.getBinding)) yield computeField(parent, ad.getName, computeType(codec.schema, ad))
        }
      }
    }
  }

  def subclassByRestriction(child: PropertyType) =
    isDescendedFrom(child, GMLSchema.FEATUREPROPERTYTYPE_TYPE)

  def allProps(ct:ComplexType):Iterable[PropertyDescriptor] = ct.getSuper match {
    case st:ComplexType if subclassByRestriction(st) => allProps(st).drop(ct.getDescriptors.size())++ct.getDescriptors
    case st:ComplexType => allProps(st)++ct.getDescriptors
    case _ => ct.getDescriptors
  }

  def isDescendedFrom(child: PropertyType, ancestor: PropertyType): Boolean =
    Iterator.iterate(child)(_.getSuper).takeWhile(_ != null).exists(_.eq(ancestor))

  @scala.annotation.tailrec
  def nearestSimpleType(at: PropertyType): PropertyType = at match {
    case ct:ComplexType => nearestSimpleType(ct.getSuper)
    case st => st
  }

  sealed trait SchemaType {
    def skip(d:Decoder)
  }

  final case class Nullable(schema: SchemaType) extends SchemaType {
    def this(schema: SchemaType, nullidx:Int) = { this(schema); this.nullidx = nullidx; }
    private[this] var nullidx: Int = 0
    def writeNull(e:Encoder) = { e.writeIndex(nullidx); e.writeNull(); }
    def writeNonNull(e:Encoder) = e.writeIndex(1 - nullidx)
    def readNull(d:Decoder) = if(d.readIndex() == nullidx) { d.readNull(); true; } else false
    def skip(d:Decoder) = if(!readNull(d)) schema.skip(d)
  }

  case class Array(schema: SchemaType) extends SchemaType {
    override def skip(d: Decoder): Unit = {
      var sz=d.skipArray()
      while(sz>0) {
        do {
          schema.skip(d)
          sz = sz - 1
        } while(sz > 0)
        sz = d.skipArray()
      }
    }
  }

  case class Record(fields: List[(Name, SchemaType)]) extends SchemaType {
    override def skip(d: Decoder): Unit = fields.foreach(_._2.skip(d))
  }

  case class DeferredRecord(r: ()=>Record) extends SchemaType {
    lazy val rec = r()

    override def skip(d: Decoder): Unit = rec.skip(d)
  }

  case class Fixed(sz: Int) extends SchemaType {
    override def skip(d: Decoder): Unit = d.skipFixed(sz)
  }

  case class Primitive(t: Schema.Type) extends SchemaType {
    override def skip(d: Decoder): Unit = t match {
      case STRING => d.skipString()
      case INT => d.readInt()
      case LONG => d.readLong()
      case FLOAT => d.readFloat()
      case DOUBLE => d.readDouble()
      case BOOLEAN => d.readBoolean()
      case NULL => d.readNull()
      case BYTES => d.skipBytes()
    }
  }

  // Map keys in Avro are always strings; not general enough for our use case
  //case class Map(k: SchemaType, v: SchemaType) extends SchemaType

  def isPrimitive(t: Schema.Type) = t match {
    case MAP|RECORD|ARRAY|UNION|ENUM => false
    case _ => true
  }

  def isNullable(s:Schema) =
    s.getType == UNION && s.getTypes.size() == 2 && s.getTypes.exists(_.getType == NULL)

  def getType(s:Schema, fne:FieldNameEncoder):SchemaType = {
    val constructed = mutable.Map.empty[Schema,Record]
    val constructing = mutable.Set.empty[Schema]
    def getType(s: Schema): SchemaType = s.getType match {
      case UNION if isNullable(s) =>
        val nullidx = s.getTypes.indexWhere(_.getType == NULL)
        val schema = s.getTypes.get(1 - nullidx)
        new Nullable(getType(schema), nullidx)
      case ARRAY =>
        Array(getType(s.getElementType))
      case RECORD if constructing(s) =>
        DeferredRecord(()=>constructed(s))
      case RECORD if constructed.contains(s) =>
        constructed(s)
      case RECORD =>
        constructing(s)=true
        val ret=Record(s.getFields.map(f=>fne.decode(f,s)->getType(f.schema())).toList)
        constructing(s)=false
        constructed(s)=ret
        ret
      case FIXED =>
        Fixed(s.getFixedSize)
      case other if isPrimitive(other) =>
        Primitive(other)
      case _ =>
        throw new IllegalStateException(s"Type not supported: $s");
    }
    getType(s)
  }
}
