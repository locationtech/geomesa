/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding

import cascading.tuple._
import com.twitter.scalding._
import org.apache.accumulo.core.data.{Key, Mutation, Range => AcRange, Value}
import org.apache.accumulo.core.util.{Pair => AcPair}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.jobs.scalding.taps.{AccumuloLocalScheme, AccumuloLocalTap, AccumuloScheme, AccumuloTap}

case class AccumuloSource(options: AccumuloSourceOptions)
    extends Source with TypedSource[(Key,Value)] with TypedSink[(Text, Mutation)] {

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case _: Hdfs  => AccumuloTap(readOrWrite, AccumuloScheme(options))
      case _: Local => AccumuloLocalTap(readOrWrite, AccumuloLocalScheme(options))
      case _: Test  => TestTapFactory(this, AccumuloScheme(options).asInstanceOf[GenericScheme]).createTap(readOrWrite)
      case _        => throw new NotImplementedError()
    }

  override def sourceFields: Fields = AccumuloSource.sourceFields

  override def converter[U >: (Key, Value)]: TupleConverter[U] = new TupleConverter[U] {
    override def arity: Int = 2
    override def apply(te: TupleEntry): (Key, Value) =
      (te.getObject(0).asInstanceOf[Key], te.getObject(1).asInstanceOf[Value])
  }

  override def sinkFields: Fields = AccumuloSource.sinkFields

  override def setter[U <: (Text, Mutation)]:  TupleSetter[U] = new TupleSetter[U] {
    override def arity: Int = 2
    override def apply(arg: U): Tuple = new Tuple(arg._1, arg._2)
  }
}

object AccumuloSource {
  def sourceFields: Fields = new Fields("k", "v")
  def sinkFields: Fields = new Fields("t", "m")
}
