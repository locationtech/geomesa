/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa.kafka

import kafka.message.MessageAndMetadata
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class MockKafkaConsumerFactory extends KafkaConsumerFactory("mock-zoo") {

  override val kafkaConsumer = mock(classOf[KafkaConsumer[Array[Byte], Array[Byte]]])

  override val offsetManager = mock(classOf[OffsetManager])
}

object MockKafkaStream {

  def apply[K, V](data: Seq[MessageAndMetadata[K, V]]): KafkaStreamLike[K, V] = {
    val ksl = mock(classOf[KafkaStreamLike[K, V]])

    // Kafka consumer iterators will block until there is a new message
    // for testing, throw an exception if a consumer tries to read beyond ``data``
    val end: Iterator[MessageAndMetadata[K, V]] = new Iterator[MessageAndMetadata[K, V]] {

      override def hasNext: Boolean =
        throw new IllegalStateException("Attempting to read beyond given mock data.")

      override def next(): MessageAndMetadata[K, V] =
        throw new IllegalStateException("Attempting to read beyond given mock data.")
    }

    when(ksl.iterator).thenAnswer(new Answer[Iterator[MessageAndMetadata[K, V]]] {

      override def answer(invocation: InvocationOnMock): Iterator[MessageAndMetadata[K, V]] = {
        data.iterator ++ end
      }
    })

    ksl
  }

}