package org.locationtech.geomesa.features.avro.complex

import java.io.ByteArrayOutputStream
import java.net.URI
import java.sql.Timestamp
import java.util.UUID

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.geotools.xs.XSSchema
import org.locationtech.geomesa.features.avro.complex.AvroCodecs.{FeatureCodecs, StatefulCodecs}
import org.locationtech.geomesa.features.avro.complex.AvroSchemas.FieldNameEncoderV1
import org.locationtech.geomesa.utils.geotools.builder.{FeatureBuilder, FeatureTypeBuilder}
import org.opengis.feature.`type`.FeatureType

import scala.collection.JavaConverters._

object ComplexEncoderTest {
  def main(args:Array[String]) = {
    val geomfac = new GeometryFactory(new PrecisionModel())

    val hubType = FeatureTypeBuilder.namespace("urn:foo")
      .`type`("HubType")
      .string("material")
      .end

    val wheelType = FeatureTypeBuilder.namespace("urn:foo")
      .`type`("WheelType")
      .intProp("radius")
      .feature("hub", hubType)
      .end

    lazy val carType:FeatureType = FeatureTypeBuilder.namespace("urn:foo")
      .`type`("CarType")
      .multi.feature("wheels", wheelType)
      .optional.feature("spare", wheelType)
      .optional.geometry("geometry")
      .optional.point("point")
      .optional.linestring("linestring")
      .optional.polygon("polygon")
      .optional.multiGeometry("multiGeometry")
      .optional.multiPoint("multiPoint")
      .optional.multiLinestring("multiLinestring")
      .optional.multiPolygon("multiPolygon")
      .optional.bool("isAcWorking")
      .optional.string("manufacturer")
      .optional.property(XSSchema.DATETIME_TYPE, "manufactureDate")
      .optional.deferredFeature("yodawg", carType)
      .optional.list[Integer]("bunchOfNumbers")
      .optional.list[UUID]("bunchOfUUIDs")
      .optional.map[String,UUID]("mapOfUUIDs")
      .optional.multi.map[String,Timestamp]("lotsOfMaps")
      .end

    val car = FeatureBuilder(carType) { _
      .set("name") { _
        .set("simpleContent","foo")
        .set("codeSpace",URI.create("urn:foo:awesome:codespace"))
      }
      .set("description", "Well it's a car.")
      .set("wheels") { _
        .set("Wheel") { _
          .set("radius", 25)
          .set("hub") { _
            .set("Hub") { _
              .set("material", "mahogany")
            }
          }
        }
      }
      .set("wheels") { _
        .set("Wheel") { _
          .set("radius", 25)
          .set("hub") { _
            .set("Hub") { _
              .set("material", "chrome")
            }
          }
        }
      }
      .set("wheels") { _
        .set("Wheel") { _
          .set("radius", 25)
          .set("hub") { _
            .set("Hub") { _
              .set("material", "AOL CDs")
            }
          }
        }
      }
      .set("wheels") { _
        .set("Wheel") { _
          .set("radius", 25)
          .set("hub") { _
            .set("Hub") { _
              .set("material", "comedy gold")
            }
          }
        }
      }
      .set("spare") { _
        .set("Wheel") { _
          .set("radius", 12)
          .set("hub") { _
            .set("Hub") { _
              .set("material", "sadness")
            }
          }
        }
      }
      .set("isAcWorking", false)
      .set("manufacturer", "Yotota")
      .set("manufactureDate", new Timestamp(System.currentTimeMillis()))
      .set("geometry", geomfac.createPoint(new Coordinate(0,0)))
      .set("yodawg") { _
          .set("Car") { _
              .set("description", "There's a car in your car so you can drive while you drive")
              .set("manufacturer", "Xzibit")
          }
      }
      .set("bunchOfNumbers", List(1,3,4,7,11,18,29).asJava)
      .set("bunchOfUUIDs", List.fill(4)(UUID.randomUUID()).asJava)
      .set("mapOfUUIDs", Map("foo"->UUID.randomUUID(), "bar"->UUID.randomUUID()).asJava)
      .set("lotsOfMaps", Map("now"->new Timestamp(System.currentTimeMillis())).asJava)
      .set("lotsOfMaps", Map("then"->new Timestamp(0)).asJava)
    }

    val reg = new { val fne = FieldNameEncoderV1 } with StatefulCodecs(new WKBWriter(), new WKBReader()) with FeatureCodecs
    val schema = reg.schemaFor(carType)
    System.out.println(schema.toString(true))

    val codec = reg.find(car.getDescriptor)
    val baos = new ByteArrayOutputStream()

    val encoder = EncoderFactory.get().directBinaryEncoder(baos,null)
    //new AvroEncoder(validator).encodeComplexAttribute(schema, wheel)

    codec.enc(car,encoder)

    encoder.flush()
    val bytes = baos.toByteArray
    val genericData = GenericData.get().createDatumReader(schema)
      .asInstanceOf[GenericDatumReader[Any]]
      .read(null, DecoderFactory.get().validatingDecoder(schema,DecoderFactory.get().binaryDecoder(bytes, null)))

    System.out.println(genericData)

    baos.reset()
    val avroenc = EncoderFactory.get().binaryEncoder(baos, null)
    for(_<-0 until 500000) {
      baos.reset()
      codec.enc(car, EncoderFactory.get().binaryEncoder(baos, avroenc))
    }

    val avrodec = DecoderFactory.get().binaryDecoder(bytes,null)
    for(_<-0 until 500000) {
      codec.dec(DecoderFactory.get().binaryDecoder(bytes,avrodec))
    }
    val foo = codec.dec(DecoderFactory.get().binaryDecoder(bytes,avrodec))
    System.out.println("Done")
  }
}
