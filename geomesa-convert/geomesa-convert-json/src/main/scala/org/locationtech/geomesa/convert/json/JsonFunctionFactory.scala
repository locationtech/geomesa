package org.locationtech.geomesa.convert.json

import com.google.gson.JsonElement
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert.{TransformerFn, TransformerFunctionFactory}

class JsonFunctionFactory extends TransformerFunctionFactory {
  private val json2string = new TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      args(0).asInstanceOf[JsonElement].toString

    override def name: String = "json2string"
  }

  override val functions: Seq[TransformerFn] = Seq(json2string)
}
