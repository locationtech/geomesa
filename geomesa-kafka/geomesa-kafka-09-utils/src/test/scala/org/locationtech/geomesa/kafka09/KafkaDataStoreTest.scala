package org.locationtech.geomesa.kafka09
/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/


import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka
import org.specs2.runner.JUnitRunner

// All tests run with kafka 0.8 should pass here with kafka 0.9
@RunWith(classOf[JUnitRunner]) class KafkaDataStoreTest extends kafka.KafkaDataStoreTest
@RunWith(classOf[JUnitRunner]) class KafkaConsumerTest extends kafka.consumer.KafkaConsumerTest
@RunWith(classOf[JUnitRunner]) class OffsetManagerIntegrationTest extends kafka.consumer.offsets.OffsetManagerIntegrationTest
@RunWith(classOf[JUnitRunner]) class OffsetTest extends kafka.consumer.offsets.OffsetTest
@RunWith(classOf[JUnitRunner]) class RequestedOffsetTest extends kafka.consumer.offsets.RequestedOffsetTest
@RunWith(classOf[JUnitRunner]) class GeoMessageTest extends kafka.GeoMessageTest
@RunWith(classOf[JUnitRunner]) class KafkaDataStoreHelperTest extends kafka.KafkaDataStoreHelperTest
@RunWith(classOf[JUnitRunner]) class KafkaDataStoreSchemaManagerTest extends kafka.KafkaDataStoreSchemaManagerTest
@RunWith(classOf[JUnitRunner]) class LiveFeatureCacheTest extends kafka.LiveFeatureCacheTest
@RunWith(classOf[JUnitRunner]) class LiveKafkaConsumerFeatureSourceTest extends kafka.LiveKafkaConsumerFeatureSourceTest
@RunWith(classOf[JUnitRunner]) class ReplayConfigTest extends kafka.ReplayConfigTest
@RunWith(classOf[JUnitRunner]) class ReplayKafkaConsumerFeatureSourceTest extends kafka.ReplayKafkaConsumerFeatureSourceTest
@RunWith(classOf[JUnitRunner]) class ReplayKafkaDataStore extends kafka.ReplayKafkaDataStoreTest
@RunWith(classOf[JUnitRunner]) class TestReplaySnapshotFeatureCacheTest extends kafka.ReplaySnapshotFeatureCacheTest
@RunWith(classOf[JUnitRunner]) class ReplayTimeHelperTest extends kafka.ReplayTimeHelperTest
@RunWith(classOf[JUnitRunner]) class TimestampFilterSplitTest extends kafka.TimestampFilterSplitTest
