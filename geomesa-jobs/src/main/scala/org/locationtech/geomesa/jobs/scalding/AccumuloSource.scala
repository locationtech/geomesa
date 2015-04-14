/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
