/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.index

import org.apache.accumulo.core.client.BatchScanner
import org.apache.accumulo.core.data.Key

trait IndexEntryEncoder[E] {
  def encode(entry: E): Seq[KeyValuePair]
}
trait IndexEntryDecoder[E] {
  def decode(key: Key): E
}

trait Filter

trait QueryPlanner[E] {
  def planQuery(bs: BatchScanner, filter: Filter): BatchScanner
}

trait IndexSchema[E] {
  val encoder: IndexEntryEncoder[E]
  val decoder: IndexEntryDecoder[E]
  val planner: QueryPlanner[E]

  def encode(entry: E) = encoder.encode(entry)
  def decode(key: Key): E = decoder.decode(key)
  def planQuery(bs: BatchScanner, filter: Filter) = planner.planQuery(bs, filter)
}
