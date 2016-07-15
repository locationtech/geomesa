package org.locationtech.geomesa.features.avro.complex

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.{net, sql, util}
import java.util.function.Function
import java.util.{Calendar, Date, GregorianCalendar, UUID, ArrayList => JArrayList, Collection => JCollection, Collections => JCollections, HashMap => JHashMap}

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import org.apache.avro.Schema
import org.apache.avro.io.{Decoder, Encoder}
import org.geotools.feature.`type`.AttributeDescriptorImpl
import org.geotools.feature.{AttributeImpl, ComplexAttributeImpl, FeatureImpl}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.gml3.GMLSchema
import org.locationtech.geomesa.features.avro.complex.AvroSchemas.{FieldNameEncoder, _}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._
import org.opengis.feature.`type`.{PropertyDescriptor, _}
import org.opengis.feature.{Attribute, ComplexAttribute, Property}
import org.opengis.filter.identity.FeatureId

import scala.collection.mutable
import scala.reflect.ClassTag

object AvroCodecs {

  val primitiveTypes =
    List(
      classOf[String],
      classOf[java.lang.Integer],
      classOf[Int],
      classOf[java.lang.Long],
      classOf[Long],
      classOf[java.lang.Double],
      classOf[Double],
      classOf[java.lang.Float],
      classOf[Float],
      classOf[java.lang.Boolean],
      classOf[Boolean]
    )

  abstract case class AvroCodec[T](schema:Schema) {
    val clazz: Class[T]
    def enc(t: T, enc: Encoder): Unit
    def dec(dec: Decoder): T
  }

  trait CodecRegistry {
    protected val registry = mutable.Map.empty[Class[_],mutable.LinkedHashMap[Schema.Type,AvroCodec[_]]]
    protected val cache = mutable.Map.empty[Class[_], Option[mutable.LinkedHashMap[Schema.Type,AvroCodec[_]]]]

    def bind[T](encMethod: (Encoder,T)=>Unit, decMethod: (Decoder)=>T, schema: Schema)(implicit tag: ClassTag[T]) = {
      val codec = new AvroCodec[T](schema) {
        val clazz = tag.runtimeClass.asInstanceOf[Class[T]]
        override def enc(t: T, out: Encoder): Unit = encMethod(out, t)
        override def dec(in: Decoder): T = decMethod(in)
      }
      registry.getOrElseUpdate(tag.runtimeClass,mutable.LinkedHashMap.empty)(schema.getType) = codec
      codec
    }

    val JBOOLEAN = classOf[java.lang.Boolean]
    val JBYTE = classOf[java.lang.Byte]
    val JCHAR = classOf[java.lang.Character]
    val JSHORT = classOf[java.lang.Short]
    val JINT = classOf[java.lang.Integer]
    val JLONG = classOf[java.lang.Long]
    val JFLOAT = classOf[java.lang.Float]
    val JDOUBLE = classOf[java.lang.Double]

    def normalizePrimitive(clazz:Class[_]) = clazz match {
      case JBOOLEAN | java.lang.Boolean.TYPE => classOf[Boolean]
      case JBYTE | java.lang.Byte.TYPE => classOf[Byte]
      case JCHAR | java.lang.Character.TYPE => classOf[Char]
      case JSHORT | java.lang.Short.TYPE => classOf[Short]
      case JINT | java.lang.Integer.TYPE => classOf[Int]
      case JLONG | java.lang.Long.TYPE => classOf[Long]
      case JFLOAT | java.lang.Float.TYPE => classOf[Float]
      case JDOUBLE | java.lang.Double.TYPE => classOf[Double]
      case x => x
    }

    def findAll[T](clazz:Class[T]): Option[mutable.LinkedHashMap[Schema.Type, AvroCodec[_]]] =
      cache.getOrElseUpdate(clazz, registry.get(normalizePrimitive(clazz))
        .orElse(Option(clazz.getSuperclass).flatMap(findAll(_)))
        .orElse {
          for (ifc <- clazz.getInterfaces; map <- findAll(ifc))
            return Some(map)
          None
        }
      )

    def find[T](clazz:Class[T], s:Schema.Type): Option[AvroCodec[_>:T]] =
      findAll(clazz).flatMap(m=>m.get(s)).asInstanceOf[Option[AvroCodec[_>:T]]]

    def find[T](clazz:Class[T]): Option[AvroCodec[_>:T]] =
      findAll(clazz).flatMap(m=>m.headOption).map{case(t,c)=>c}.asInstanceOf[Option[AvroCodec[_>:T]]]
  }

  trait PrimitiveCodecs extends CodecRegistry {
    implicit val STRING = bind(_.writeString(_:String), _.readString(), Schema.create(Schema.Type.STRING))
    implicit val BYTES = bind(_.writeBytes(_:scala.Array[Byte]), d=>{
      val buf = d.readBytes(null)
      val pos = buf.position()
      val len = buf.limit() - pos
      if(len == 0)
        scala.Array.emptyByteArray
      else if(pos == 0 && buf.hasArray)
        buf.array()
      else {
        val a = scala.Array.ofDim[Byte](len)
        buf.get(a, 0, len)
        a
      }
    }, Schema.create(Schema.Type.BYTES))
    implicit val BYTEBUFFER = bind(_.writeBytes(_:ByteBuffer), _.readBytes(null), Schema.create(Schema.Type.BYTES))
    implicit val BOOLEAN = bind(_.writeBoolean(_:Boolean), _.readBoolean(), Schema.create(Schema.Type.BOOLEAN))

    val BYTE_AS_INT = bind[Byte](_.writeInt(_), _.readInt().toByte, Schema.create(Schema.Type.INT))
    val SHORT_AS_INT = bind[Short](_.writeInt(_), _.readInt().toShort, Schema.create(Schema.Type.INT))
    implicit val INT = bind[Int](_.writeInt(_), _.readInt(),  Schema.create(Schema.Type.INT))
    val LONG_AS_INT = bind[Long]((e,l)=>e.writeInt(l.toInt), _.readInt(),  Schema.create(Schema.Type.INT))
    val FLOAT_AS_INT = bind[Float]((e,l)=>e.writeInt(l.toInt), _.readInt(),  Schema.create(Schema.Type.INT))
    val DOUBLE_AS_INT = bind[Double]((e,l)=>e.writeInt(l.toInt), _.readInt(),  Schema.create(Schema.Type.INT))

    val BYTE_AS_LONG = bind[Short](_.writeLong(_), _.readLong().toByte,  Schema.create(Schema.Type.LONG))
    val SHORT_AS_LONG = bind[Short](_.writeLong(_), _.readLong().toShort,  Schema.create(Schema.Type.LONG))
    val INT_AS_LONG = bind[Int](_.writeLong(_), _.readLong().toInt,  Schema.create(Schema.Type.LONG))
    implicit val LONG = bind[Long](_.writeLong(_), _.readLong(),  Schema.create(Schema.Type.LONG))
    val FLOAT_AS_LONG = bind[Float]((e,l)=>e.writeLong(l.toLong), _.readLong(),  Schema.create(Schema.Type.LONG))
    val DOUBLE_AS_LONG = bind[Double]((e,l)=>e.writeLong(l.toLong), _.readLong(),  Schema.create(Schema.Type.LONG))

    val BYTE_AS_FLOAT = bind[Byte](_.writeFloat(_), _.readFloat().toByte,  Schema.create(Schema.Type.FLOAT))
    val SHORT_AS_FLOAT = bind[Short](_.writeFloat(_), _.readFloat().toShort,  Schema.create(Schema.Type.FLOAT))
    val INT_AS_FLOAT = bind[Int](_.writeFloat(_), _.readFloat().toInt,  Schema.create(Schema.Type.FLOAT))
    val LONG_AS_FLOAT = bind[Long](_.writeFloat(_), _.readFloat().toLong,  Schema.create(Schema.Type.FLOAT))
    implicit val FLOAT = bind[Float](_.writeFloat(_), _.readFloat(),  Schema.create(Schema.Type.FLOAT))
    val DOUBLE_AS_FLOAT = bind[Double]((e,f)=>e.writeFloat(f.toFloat), _.readFloat(),  Schema.create(Schema.Type.FLOAT))

    val BYTE_AS_DOUBLE = bind[Byte](_.writeDouble(_), _.readDouble().toByte,  Schema.create(Schema.Type.DOUBLE))
    val SHORT_AS_DOUBLE = bind[Short](_.writeDouble(_), _.readDouble().toShort,  Schema.create(Schema.Type.DOUBLE))
    val INT_AS_DOUBLE = bind[Int](_.writeDouble(_), _.readDouble().toInt,  Schema.create(Schema.Type.DOUBLE))
    val LONG_AS_DOUBLE = bind[Long](_.writeDouble(_), _.readDouble().toLong,  Schema.create(Schema.Type.DOUBLE))
    val FLOAT_AS_DOUBLE = bind[Float](_.writeDouble(_), _.readDouble().toFloat,  Schema.create(Schema.Type.DOUBLE))
    implicit val DOUBLE = bind[Double](_.writeDouble(_), _.readDouble(),  Schema.create(Schema.Type.DOUBLE))
  }

  trait DerivedCodecs extends PrimitiveCodecs {
    def derive[T,U](encMethod: T=>U, decMethod: U=>T)(implicit primitiveCodec: AvroCodec[U], tag: ClassTag[T]) = {
      val codec = new AvroCodec[T](primitiveCodec.schema) {
        override val clazz = tag.runtimeClass.asInstanceOf[Class[T]]
        override def enc(t: T, out: Encoder) = primitiveCodec.enc(encMethod(t), out)
        override def dec(in: Decoder) = decMethod(primitiveCodec.dec(in))
      }
      registry.getOrElseUpdate(tag.runtimeClass, mutable.LinkedHashMap.empty)(primitiveCodec.schema.getType)=codec
      codec
    }

    implicit val DATE = derive[Date,Long](_.getTime, new Date(_))
    implicit val CALENDAR = derive[Calendar,Long](_.getTimeInMillis, d=>{val gc=new GregorianCalendar(); gc.setTimeInMillis(d); gc})
    implicit val SQL_DATE = derive[sql.Date,Long](_.getTime, new sql.Date(_))
    implicit val SQL_TIMESTAMP = derive[Timestamp,Long](_.getTime, new Timestamp(_))
    implicit val _UUID = derive(encodeUUID, decodeUUID)
    implicit val URI = derive[net.URI,String](_.toString, net.URI.create)
  }

  class StatefulCodecs(wkbWriter: WKBWriter, wkbReader: WKBReader) extends DerivedCodecs {
    val GEOMETRY = derive[Geometry,scala.Array[Byte]](wkbWriter.write, wkbReader.read)
  }

  def encodeUUID(uuid: UUID) =
    ByteBuffer.allocate(16)
      .putLong(uuid.getMostSignificantBits)
      .putLong(uuid.getLeastSignificantBits)
      .flip.asInstanceOf[ByteBuffer]

  def decodeUUID(bb: ByteBuffer): UUID = new UUID(bb.getLong, bb.getLong)


  /*
   * These classes are specializations of Function to improve performance
   */

  private[this] abstract class EncodeComplexAttribute extends ((ComplexAttribute,JHashMap[Name,JArrayList[Property]], Encoder)=>Unit) {
    def apply(ca:ComplexAttribute, props: JHashMap[Name,JArrayList[Property]], e:Encoder)
  }

  private[this] abstract class EncodeProperties extends ((JArrayList[Property],Encoder)=>Unit) {
    def apply(props:JArrayList[Property], e:Encoder)
  }

  private[this] abstract class EncodeProperty extends ((Encoder,Property)=>Unit) {
    def apply(e: Encoder, p: Property)
  }

  private[this] abstract class DecodeToAttribute extends (Decoder=>Attribute) {
    def apply(d:Decoder): Attribute
  }

  private[this] abstract class DecodeToAttributes extends (Decoder=>JCollection[Attribute]) {
    def apply(d:Decoder): JCollection[Attribute]
  }

  private[this] val arrayFactory = new Function[Name,JArrayList[Property]] { def apply(t: Name) = new JArrayList[Property]() }

  trait FeatureCodecs { self: CodecRegistry =>
    protected[this] val fne: FieldNameEncoder
    private[this] val schemaFac = new SchemaFactory(fne, this)
    private[this] val codecs = mutable.Map.empty[AttributeDescriptor,AvroCodec[Property]]
    private[this] val constructing = mutable.Set.empty[Record]
    private[this] val encoders = mutable.Map.empty[Record,EncodeProperty]
    private[this] val decoders = mutable.Map.empty[Record, DecodeToAttribute]

    def schemaFor(ct:ComplexType) = schemaFac(ct)

    def find(ct:ComplexType): AvroCodec[Property] = {
      find(new AttributeDescriptorImpl(ct, ct.getName, 1, 1, false, null))
    }

    def find(ad:AttributeDescriptor): AvroCodec[Property] = {
      codecs.getOrElseUpdate(ad, {
        val schema = schemaFac(ad.getType.asInstanceOf[ComplexType])
        val schemaType = getType(schema, fne)
        val enc = bindEnc(schemaType, ad)
        val dec = bindDec(schemaType, ad)
        bind[Property](enc,dec,schema)
      })
    }

    private[this] def bindEnc(s:SchemaType, ad:PropertyDescriptor):EncodeProperty = s match {
      case r@Record(fields) if ad.getType.isInstanceOf[ComplexType] =>
        val ct = ad.getType.asInstanceOf[ComplexType]
      encoders.get(r) match {
        case Some(enc) => return enc
        case _ =>
      }

      if(constructing(r)) {
        return new EncodeProperty {
          private[this] lazy val binding=encoders(r)
          override def apply(e: Encoder, p: Property): Unit = binding(e,p)
        }
      }

      constructing(r) = true

      val descByName = AvroSchemas.allProps(ct).map(d => d.getName -> d).toMap

      // Each field MUST be mapped
      val bindings = r.fields.map { case (name, st) =>
        if (AvroSchemas.AVRO_NAMESPACE == name.getNamespaceURI) {
          name.getLocalPart match {
            case "__version__" =>
              new EncodeComplexAttribute {
                def apply(ca: ComplexAttribute, p: JHashMap[Name, JArrayList[Property]], e: Encoder) = e.writeInt(1)
              }

            case "__fid__" =>
              new EncodeComplexAttribute {
                def apply(ca: ComplexAttribute, p: JHashMap[Name, JArrayList[Property]], e: Encoder) =
                  e.writeString(Option(ca.getIdentifier) flatMap (i => Option(i.getID)) map (_.toString) getOrElse "")
              }

            case "simpleContent" => st match {
              case n@Nullable(Primitive(t)) =>
                val binding = bindEncPrimitive(AvroSchemas.nearestSimpleType(ct), t)
                new EncodeComplexAttribute {
                  def apply(ca: ComplexAttribute, p: JHashMap[Name, JArrayList[Property]], e: Encoder) =
                    Option(ca.getProperty(GEOT_SIMPLE_CONTENT)) match {
                      case Some(value) if value.getValue != null =>
                        n.writeNonNull(e)
                        binding(e, value)
                      case _ =>
                        n.writeNull(e)
                    }
                }

              case _ =>
                throw new IllegalStateException(s"The property $name must be a nullable primitive type")
            }

            case other => st match {
              case n: Nullable =>
                new EncodeComplexAttribute {
                  def apply(ca: ComplexAttribute, p: JHashMap[Name, JArrayList[Property]], e: Encoder) = n.writeNull(e)
                }
              case _ => ???
            }
          }
        } else {
          descByName.get(name) match {
            case Some(desc) =>
              val binding = bindEncMany(st, desc.asInstanceOf[AttributeDescriptor])
              new EncodeComplexAttribute {
                def apply(ca: ComplexAttribute, p: JHashMap[Name, JArrayList[Property]], e: Encoder) =
                  binding(p.computeIfAbsent(name, arrayFactory), e)
              }
            case None =>
              st match {
                case n: Nullable =>
                  new EncodeComplexAttribute {
                    def apply(ca: ComplexAttribute, p: JHashMap[Name, JArrayList[Property]], e: Encoder) = n.writeNull(e)
                  }
                case _ =>
                  throw new IllegalStateException()
              }
          }
        }
      }.toArray


      val ret = new EncodeProperty {
        def apply(e: Encoder, p: Property) = {
          val ca = p.asInstanceOf[ComplexAttribute]
          val props = new JHashMap[Name, JArrayList[Property]]()
          val pIt = ca.getProperties.iterator()
          while (pIt.hasNext) {
            val p = pIt.next()
            props.computeIfAbsent(p.getName, arrayFactory).add(p)
          }
          val sz = bindings.length
          var i = 0
          while (i < sz) {
            bindings(i)(ca, props, e)
            i = i + 1
          }
        }
      }
      encoders(r) = ret
      constructing(r) = false
      ret
    }

    private[this] def bindEncMany(s:SchemaType, ad:AttributeDescriptor):EncodeProperties = s match {
      case n@Nullable(st) =>
        val binding = bindEncMany(st,ad)
        new EncodeProperties { def apply(props: JArrayList[Property], e: Encoder) = {
          // We want to be as gentle on the GC as possible, so don't allocate a list unless absolutely necessary
          var out: JArrayList[Property] = props
          var i = 0
          val sz = props.size()
          while(i<sz) {
            if(props.get(i).getValue==null) {
              out = new JArrayList[Property](sz)
              out.addAll(props.subList(0,i))
              i=i+1
              while(i<sz) {
                val prop=props.get(i)
                if(prop.getValue!=null) {
                  out.add(prop)
                }
                i=i+1
              }
            } else {
              i=i+1
            }
          }
          if (out.isEmpty) {
            n.writeNull(e)
          } else {
            n.writeNonNull(e)
            binding(out, e)
          }
        } }
      case Array(Primitive(_)) if ad.isList =>
        val binding = bindEncOne(s, ad)
        new EncodeProperties {
          override def apply(props: JArrayList[Property], e: Encoder) = binding(e,props.get(0))
        }

      case Array(Record(_)) if ad.isMap =>
        val binding = bindEncOne(s, ad)
        new EncodeProperties {
          override def apply(props: JArrayList[Property], e: Encoder) = binding(e,props.get(0))
        }

      case Array(st) =>
        val binding = bindEncOne(st,ad)
        new EncodeProperties { override def apply(props: JArrayList[Property], e: Encoder) = {
          e.writeArrayStart()
          e.setItemCount(props.size)
          var i=0
          val sz=props.size()
          while(i<sz) {
            e.startItem()
            binding(e, props.get(i))
            i=i+1
          }
          e.writeArrayEnd()
        }
        }
      case other =>
        val binding = bindEncOne(other, ad)
        new EncodeProperties { override def apply(props: JArrayList[Property], e: Encoder) = binding(e,props.get(0)) }
      case _ => ???
    }

    private[this] def bindEncOne(s:SchemaType, ad:AttributeDescriptor):EncodeProperty = s match {
      case n@Nullable(st) =>
        val binding = bindEncOne(st,ad)
        new EncodeProperty { def apply(e: Encoder, p: Property) ={
          if(p.getValue==null)
            n.writeNull(e)
          else {
            n.writeNonNull(e)
            binding(e,p)
          }
        } }

      case Array(Primitive(t)) if ad.isList =>
        val clazz = ad.getListType().get
        val Some(codec) = self.find(clazz, t)
        new EncodeProperty { def apply(e: Encoder, p: Property) ={
          e.writeArrayStart()
          val list = p.getValue.asInstanceOf[java.util.List[AnyRef]]
          e.setItemCount(list.size())
          val it=list.iterator()
          while(it.hasNext) {
            val o=it.next()
            e.startItem()
            codec.asInstanceOf[AvroCodec[AnyRef]].enc(o,e)
          }
          e.writeArrayEnd()
        } }

      case Array(Record( (_,Primitive(keyType)) :: (_,Primitive(valType)) :: _ )) if ad.isMap =>
        val Some((keyClass,valClass)) = ad.getMapTypes()
        val Some(keyCodec) = self.find(keyClass,keyType)
        val Some(valCodec) = self.find(valClass,valType)
        new EncodeProperty { def apply(e: Encoder, p: Property) ={
          e.writeArrayStart()
          val map = p.getValue.asInstanceOf[java.util.Map[AnyRef,AnyRef]]
          e.setItemCount(map.size())
          val it = map.entrySet().iterator()
          while(it.hasNext) {
            val ent = it.next()
            e.startItem()
            keyCodec.asInstanceOf[AvroCodec[AnyRef]].enc(ent.getKey,e)
            valCodec.asInstanceOf[AvroCodec[AnyRef]].enc(ent.getValue,e)
          }
          e.writeArrayEnd()
        } }

      case Array(st) =>
        val binding = bindEncOne(st,ad)
        new EncodeProperty { def apply(e: Encoder, p: Property) ={
          e.writeArrayStart()
          e.setItemCount(1)
          e.startItem()
          binding(e,p)
          e.writeArrayEnd()
        } }

      case dr:DeferredRecord =>
        bindEncOne(dr.rec, ad)

      case r:Record if ad.getType.isInstanceOf[ComplexType] =>
        bindEnc(r, ad)

      case Primitive(t) =>
        bindEncPrimitive(ad.getType, t)

      case _ =>
        ???
    }

    private[this] def bindEncPrimitive(pt:PropertyType, t:Schema.Type): EncodeProperty =
      bindEncClass(pt.getBinding, t)

    private[this] def bindEncClass(clazz:Class[_], t:Schema.Type): EncodeProperty =
      self.find(clazz, t) match {
        case Some(codec) =>
          bindEncCodec(codec)
        case None =>
          ???
      }

    private[this] def bindEncCodec[T](codec:AvroCodec[T]): EncodeProperty =
      new EncodeProperty { override def apply(e: Encoder, p: Property) = codec.enc(p.getValue.asInstanceOf[T],e) }


    private[this] def bindDec(s:SchemaType, ad:PropertyDescriptor): DecodeToAttribute = s match {
      case r@Record(fields) =>
        val ct=ad.getType.asInstanceOf[ComplexType]
        decoders.get(r) match { case Some(dec) => return dec case _ => }
        if(constructing(r)) {
          return new DecodeToAttribute {
            private[this] lazy val binding=bindDec(r,ad)
            def apply(d: Decoder) = binding(d)
          }
        }
        constructing(r)=true
        val descByName=AvroSchemas.allProps(ct).map(d=>d.getName->d).toMap
        val isFeature = isDescendedFrom(ct, GMLSchema.ABSTRACTFEATURETYPE_TYPE)

        abstract class Builder() {
          @inline private[FeatureCodecs] final val props:JArrayList[Property] = new JArrayList[Property]()
          final var id:FeatureId = null
          def build(): ComplexAttribute
        }

        val builderFac: ()=>Builder = if(isFeature) ()=>new Builder() {
          override def build(): ComplexAttribute = new FeatureImpl(props, ad.asInstanceOf[AttributeDescriptor], id)
        } else ()=>new Builder() {
          override def build(): ComplexAttribute = new ComplexAttributeImpl(props, ad.asInstanceOf[AttributeDescriptor], id)
        }

        abstract class Applier {
          def apply(d:Decoder, props:JArrayList[Property], b:Builder): Unit
        }

        val bindings:scala.Array[Applier] = (for((name,st)<-r.fields) yield {
          lazy val fieldSkipper = new Applier {
            def apply(d:Decoder,props:JArrayList[Property], b:Builder)=st.skip(d)
          }

          if(AVRO_NAMESPACE == name.getNamespaceURI) name.getLocalPart match {
            case "__version__" =>
              new Applier {
                def apply(d:Decoder,props:JArrayList[Property],b:Builder)=d.readInt()
              }
            case "__fid__" =>
              new Applier {
                def apply(d:Decoder,props:JArrayList[Property], b:Builder)={
                  val s = d.readString()
                  if(!s.isEmpty) b.id = new FeatureIdImpl(s)
                }
              }
            case "simpleContent" => st match {
              case n@Nullable(Primitive(t)) =>
                val st=nearestSimpleType(ct)
                self.find(st.getBinding, t) match {
                  case Some(codec) =>
                    new Applier {
                      def apply(d:Decoder,props:JArrayList[Property], b:Builder)={
                        val value = if(n.readNull(d)) null else codec.dec(d)
                        props add new AttributeImpl(value, new AttributeDescriptorImpl(st.asInstanceOf[AttributeType], GEOT_SIMPLE_CONTENT, 0, 1, true, null), null)
                      }
                    }
                }
              case _ =>
                fieldSkipper
            }
            case _ =>
              fieldSkipper
          } else descByName.get(name) match {
            case Some(desc) =>
              val binding = bindDecMany(st, desc.asInstanceOf[AttributeDescriptor])
              new Applier {
                def apply(d:Decoder,props:JArrayList[Property], b:Builder)=props.addAll(binding(d))
              }
            case _ =>
              fieldSkipper
          }
        }).toArray

        val ret = new DecodeToAttribute{
          def apply(d: Decoder) = {
            val b = builderFac()
            val props = b.props
            val sz = bindings.length
            var i = 0
            while (i < sz) {
              bindings(i)(d, props, b)
              i = i + 1
            }
            b.build()
          }
        }
        decoders(r)=ret
        constructing(r)=false
        ret
      case _ =>
        ???
    }

    private[this] def bindDecMany(s: SchemaType, ad:AttributeDescriptor): DecodeToAttributes = s match {
      case n@Nullable(st) =>
        new DecodeToAttributes {
          private[this] val binding=bindDecMany(st,ad)
          override def apply(d: Decoder): JCollection[Attribute] =
            if(n.readNull(d)) {
              JCollections.emptyList[Attribute]
            } else {
              binding(d)
            }
        }
      case Array(Primitive(_)) if ad.isList =>
        new DecodeToAttributes {
          private[this] val binding=bindDecOne(s,ad)
          override def apply(d: Decoder): JCollection[Attribute] = JCollections.singletonList(binding(d))
        }

      case Array(Record(_)) if ad.isMap =>
        new DecodeToAttributes {
          private[this] val binding=bindDecOne(s,ad)
          override def apply(d: Decoder): JCollection[Attribute] = JCollections.singletonList(binding(d))
        }

      case Array(st) =>
        new DecodeToAttributes {
          private[this] val binding = bindDecMany(st, ad)

          override def apply(d: Decoder): JCollection[Attribute] = {
            val buf = new JArrayList[Attribute]()
            var sz = d.readArrayStart()
            while (sz != 0) {
              do {
                buf.addAll(binding(d))
                sz = sz - 1
              } while (sz != 0)
              sz = d.arrayNext()
            }
            buf
          }
        }

      case _ =>
        new DecodeToAttributes {
          private[this] val binding=bindDecOne(s, ad)
          override def apply(d: Decoder): JCollection[Attribute] = JCollections.singletonList(binding(d))
        }
    }

    private[this] def bindDecOne(s: SchemaType, ad: AttributeDescriptor): DecodeToAttribute = s match {
      case n@Nullable(st) =>
        val binding = bindDecOne(st,ad)
        new DecodeToAttribute {
          def apply(d: Decoder) =
            if (n.readNull(d))
              new AttributeImpl(null, ad, null)
            else
              binding(d)
        }

      case dr:DeferredRecord =>
        bindDecOne(dr.rec, ad)

      case Array(Primitive(t)) if ad.isList =>
        self.find(ad.getListType().get, t) match {
          case Some(codec) =>
            new DecodeToAttribute {
              def apply(d: Decoder) = {
                val buf = new util.ArrayList[Any]()
                var sz = d.readArrayStart()
                while (sz != 0) {
                  do {
                    buf.add(codec.dec(d))
                    sz = sz - 1
                  } while (sz != 0)
                  sz = d.arrayNext()
                }
                new AttributeImpl(buf, ad, null)
              }
            }
          case None => ???
        }

      case Array(Record( (_,Primitive(keyType)) :: (_,Primitive(valType)) :: _ )) if ad.isMap =>
        val Some((keyClass, valClass)) = ad.getMapTypes()
        val Some(keyCodec) = self.find(keyClass, keyType)
        val Some(valCodec) = self.find(valClass, valType)
        new DecodeToAttribute {
          def apply(d: Decoder) = {
            val map = new java.util.HashMap[Any, Any]()
            var sz = d.readArrayStart()
            while (sz != 0) {
              do {
                val k = keyCodec.dec(d)
                val v = valCodec.dec(d)
                map.put(k, v)
                sz = sz - 1
              } while (sz != 0)
              sz = d.arrayNext()
            }
            new AttributeImpl(map, ad, null)
          }
        }
      case r:Record =>
        bindDec(s,ad)

      case Primitive(t) =>
        self.find(ad.getType.getBinding, t) match {
          case Some(codec) =>
            new DecodeToAttribute {
              def apply(d: Decoder) = new AttributeImpl(codec.dec(d), ad, null)
            }
          case None => ???
        }

      case _ =>
        ???
    }

  }
}
