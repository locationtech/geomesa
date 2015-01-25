package org.locationtech.geomesa.convert.avro

import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.convert.avro.AvroPath.CompositeExpr
import org.locationtech.geomesa.convert.{TransformerFn, TransformerFunctionFactory}

class AvroPathFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[String] = Seq("avroPath")

  override def build(name: String): TransformerFn = AvroPathFn()

  case class AvroPathFn(var path: CompositeExpr = null) extends TransformerFn {
    override def eval(args: Any*): Any = {
      if(path == null) path = AvroPath(args(1).asInstanceOf[String])
      path.eval(args(0).asInstanceOf[GenericRecord]).orNull
    }
  }
}