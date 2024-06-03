/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import com.esotericsoftware.kryo.io.Input
import com.typesafe.scalalogging.LazyLogging
import org.json4s.JsonAST._
import org.json4s.native.JsonMethods.{parse => _, _}
import org.locationtech.geomesa.features.kryo.json.KryoJsonPath.ValuePointer

/**
 * Deserializes the results of json-paths. Not thread-safe. The input may end up positioned at arbitrary locations
 *
 * @param in input containing the serialized json
 * @param root pointer to the root document/array
 */
class KryoJsonPath(in: Input, root: ValuePointer) extends LazyLogging {

  import JsonPathParser._
  import KryoJsonPath._
  import KryoJsonSerialization._

  import scala.collection.JavaConverters._

  /**
   * Deserializes the results of json-paths. Not thread-safe. The input may end up positioned at arbitrary locations
   *
   * @param in input, positioned at the start of the json object
   */
  def this(in: Input) = this(in, KryoJsonPath.root(in))

  /**
    * Deserialize the result of a json-path.
    *
    * If the path selects leaf elements, they will be returned as primitives. If the path
    * selects objects, they will be returned as strings of json. If more than one item is
    * selected, they will be returned in a java List. If nothing is selected, it will return null.
    *
    * @param path json path to evaluate
    * @return result of the path, if any
    */
  def deserialize(path: JsonPath): Any = {
    require(path.nonEmpty, "Path must not be empty")

    if (root == null || root.typ == NullByte) {
      return null
    }

    val matches = path.elements.foldLeft(Seq(root)) {
      case (nodes, _) if nodes.isEmpty       => nodes
      case (nodes, PathAttribute(name, _))   => matchPathAttribute(nodes, Some(name))
      case (nodes, PathAttributeWildCard)    => matchPathAttribute(nodes, None)
      case (nodes, PathIndices(indices))     => matchPathIndex(nodes, indices)
      case (nodes, PathIndexRange(from, to)) => matchPathIndexRange(nodes, from, to)
      case (nodes, PathIndexWildCard)        => matchAllPathIndices(nodes)
      case (nodes, PathDeepScan)             => matchDeep(nodes)
      case (nodes, PathFilter(op))           => matchFilter(nodes, op)
    }

    if (matches.isEmpty) {
      // special handling to return 0 for length functions without matches
      path.function match {
        case Some(PathFunction.LengthFunction) => 0
        case _ => null
      }
    } else {
      val values = matches.map(m => KryoJsonPath.readPathValue(in, m.typ, m.pos))
      val result = if (values.lengthCompare(1) == 0) { values.head } else { values.asJava }
      path.function match {
        case None => result
        case Some(f) => f.apply(result)
      }
    }
  }

  // matching reads - will select further nodes to process based on an input predicate

  /**
    * Given a set of input nodes defined by their types and offsets, return a list of output nodes which
    * match the given attribute name
    *
    * @param nodes input node types and offsets
    * @param name attribute name to match - if None will match all names
    * @return matching node types and offsets
    */
  private def matchPathAttribute(nodes: Seq[ValuePointer], name: Option[String]): Seq[ValuePointer] = {
    val docs = nodes.collect { case ValuePointer(t, p) if t == DocByte || t == ArrayByte => p }
    matchObjectPath(docs, name.map(n => () => n == in.readName()))
  }

  /**
    * Given a set of input nodes defined by their types and offsets, return a list of output nodes which
    * match the given index
    *
    * @param nodes input node types and offsets
    * @param indices array indices to match
    * @return matching node types and offsets
    */
  private def matchPathIndex(nodes: Seq[ValuePointer], indices: Seq[Int]): Seq[ValuePointer] = {
    if (indices.forall(_ >= 0)) {
      matchPathIndices(nodes, Some(() => indices.contains(in.readName().toInt)))
    } else {
      // have to do post-filtering to read backwards from end
      val all = matchPathIndices(nodes, None)
      val res = Seq.newBuilder[ValuePointer]
      indices.foreach { i =>
        res += (if (i < 0) { all(all.length + i) } else { all(i) })
      }
      res.result()
    }
  }

  /**
   * Given a set of input nodes defined by their types and offsets, return a list of output nodes which
   * match the given index range
   *
   * @param nodes input node types and offsets
   * @param from from index, inclusive, or None to start from the first element
   * @param to to index, exclusive, or None to end with the last element
   * @return matching node types and offsets
   */
  private def matchPathIndexRange(nodes: Seq[ValuePointer], from: Option[Int], to: Option[Int]): Seq[ValuePointer] = {
    if (from.forall(_ >= 0) && to.forall(_ >= 0)) {
      matchPathIndices(nodes, Some(() => { val i = in.readName().toInt; from.forall(_ <= i) && to.forall(_ > i) }))
    } else {
      // have to do post-filtering to read backwards from end
      val all = matchPathIndices(nodes, None)
      def offset(bound: Int): Int = if (bound >= 0) { bound } else { all.length + bound }
      all.slice(from.map(offset).getOrElse(0), to.map(offset).getOrElse(all.length))
    }
  }

  /**
   * Given a set of input nodes defined by their types and offsets, return a list of output nodes
   *
   * @param nodes input node types and offsets
   * @return matching node types and offsets
   */
  private def matchAllPathIndices(nodes: Seq[ValuePointer]): Seq[ValuePointer] =
    matchPathIndices(nodes, None)

  private def matchPathIndices(nodes: Seq[ValuePointer], predicate: Option[() => Boolean]): Seq[ValuePointer] =
    matchObjectPath(nodes.collect { case ValuePointer(ArrayByte, p) => p }, predicate)

  /**
    * Given a set of input nodes defined by their offsets, return a list of matching nodes
    * which match the predicate. Input nodes must be either arrays or objects
    *
    * Note: predicate must consume the 'name' from the input stream
    *
    * @param positions input node offsets - must be arrays or objects
    * @param nameConsumingPredicate optional predicate to match - must consume the object name from the input stream
    * @return matching node types and offsets
    */
  private def matchObjectPath(positions: Seq[Int], nameConsumingPredicate: Option[() => Boolean]): Seq[ValuePointer] = {
    val predicate: () => Boolean = nameConsumingPredicate.getOrElse(() => { in.skipName(); true })
    val result = Seq.newBuilder[ValuePointer]
    positions.foreach { position =>
      in.setPosition(position)
      val end = position + in.readInt() - 1 // last byte is the terminal byte
      while (in.position() < end) {
        val switch = in.readByte()
        if (predicate()) {
          result += ValuePointer(switch, in.position())
        }
        switch match {
          case StringByte   => skipString(in)
          case DocByte      => skipDocument(in)
          case ArrayByte    => skipDocument(in) // arrays are stored as docs
          case DoubleByte   => in.skip(8)
          case IntByte      => in.skip(4)
          case LongByte     => in.skip(8)
          case NullByte     => // no-op
          case BooleanByte  => skipBoolean(in)
        }
      }
    }
    result.result()
  }

  /**
   * Given a set of input nodes defined by their types and offsets, returns all nodes contained by the inputs.
   *
   * Note: this is implemented as a depth-first search to match jayway json-path behavior
   *
   * @param nodes input node types and offsets
   * @return matching node types and offsets
   */
  private def matchDeep(nodes: Seq[ValuePointer]): Seq[ValuePointer] = {
    nodes.flatMap { node =>
      node.typ match {
        case DocByte | ArrayByte => Seq(node) ++ matchDeep(matchObjectPath(Seq(node.pos), None))
        case _ => Seq(node) // primitives
      }
    }
  }

  /**
   * Given a set of input nodes defined by their types and offsets, return all nodes which match the given filter
   *
   * @param nodes input node types and offsets
   * @param op filter op
   * @return matching node types and offsets
   */
  private def matchFilter(nodes: Seq[ValuePointer], op: PathFilter.FilterOp): Seq[ValuePointer] = {
    val predicate = KryoJsonPathFilter(in, root, op)
    nodes.flatMap {
      case ValuePointer(ArrayByte, pos) => matchObjectPath(Seq(pos), None).filter(predicate.apply)
      case p if predicate(p) => Seq(p)
      case _ => Nil
    }
  }
}

object KryoJsonPath {

  import KryoJsonSerialization._

  import scala.collection.JavaConverters._

  /**
   * Pointer to a serialized value
   *
   * @param typ the type of the value, one of the constants defined in KryoJsonSerialization
   * @param pos the start position of the value in the serialized json bytes
   */
  private[json] case class ValuePointer(typ: Byte, pos: Int)

  /**
   * Gets the root element that the input is currently pointing to
   *
   * @param in input
   * @return
   */
  private def root(in: Input): ValuePointer = {
    in.readByte match {
      case BooleanTrue  => ValuePointer(DocByte, in.position())
      case BooleanFalse => ValuePointer(NullByte, -1)
      case NonDoc =>
        val switch = in.readByte()
        in.skipName()
        ValuePointer(switch, in.position())
    }
  }

  /**
   * Read the value for a given node path - does not return the wrapped JElement
   *
   * @param in input
   * @param typed type of value being read
   * @param position position of the value being read in the input
   * @return value
   */
  private def readPathValue(in: Input, typed: Byte, position: Int): Any = {
    import org.json4s.native.JsonMethods._

    in.setPosition(position)
    typed match {
      case StringByte   => readString(in)
      case DocByte      => compact(render(readDocument(in)))
      case ArrayByte    => unwrapArray(readArray(in))
      case DoubleByte   => in.readDouble()
      case IntByte      => in.readInt()
      case LongByte     => in.readLong()
      case NullByte     => null
      case BooleanByte  => readBoolean(in)
    }
  }

  /**
   * Unwrap a JArray into the primitive types contained within. Compared to JArray.values, this
   * serializes documents into strings
   *
   * @param array array to unwrap
   * @return
   */
  private def unwrapArray(array: JArray): java.util.List[Any] = {
    array.arr.map {
      case JString(s)              => s
      case j: JObject              => compact(render(j))
      case j: JArray               => unwrapArray(j)
      case JDouble(d)              => d
      case JInt(i) if i.isValidInt => i.intValue // note: this check needs to be a separate line to avoid auto-casting to long
      case JInt(i)                 => i.longValue
      case JLong(i)                => i
      case JNull                   => null
      case JBool(b)                => b
    }.asJava
  }
}
