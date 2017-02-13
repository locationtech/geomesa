package org.apache.spark.sql

/**
  * Created by mzimmerman on 2/8/17.
  */
object SQLFunctionHelper {
  def nullableUDF[A1, RT](f: A1 => RT): A1 => RT = {
    in1 => in1 match {
      case null => null.asInstanceOf[RT]
      case out1 => f(out1)
    }
  }

  def nullableUDF[A1, A2, RT](f: (A1, A2) => RT): (A1, A2) => RT = {
    (in1, in2) => (in1, in2) match {
      case (null, _) => null.asInstanceOf[RT]
      case (_, null) => null.asInstanceOf[RT]
      case (out1, out2) => f(out1, out2)
    }
  }

  def nullableUDF[A1, A2, A3, RT](f: (A1, A2, A3) => RT): (A1, A2, A3) => RT = {
    (in1, in2, in3) => (in1, in2, in3) match {
      case (null, _, _) => null.asInstanceOf[RT]
      case (_, null, _) => null.asInstanceOf[RT]
      case (_, _, null) => null.asInstanceOf[RT]
      case (out1, out2, out3) => f(out1, out2, out3)
    }
  }

  def nullableUDF[A1, A2, A3, A4, RT](f: (A1, A2, A3, A4) => RT): (A1, A2, A3, A4) => RT = {
    (in1, in2, in3, in4) => (in1, in2, in3, in4) match {
      case (null, _, _, _) => null.asInstanceOf[RT]
      case (_, null, _, _) => null.asInstanceOf[RT]
      case (_, _, null, _) => null.asInstanceOf[RT]
      case (_, _, _, null) => null.asInstanceOf[RT]
      case (out1, out2, out3, out4) => f(out1, out2, out3, out4)
    }
  }
}
