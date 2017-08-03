/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.tranquility.kinesis.model;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.collect.ImmutableSet;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 * Configuration object which extends Tranquility configuration with Kafka specific parameters.
 */
public abstract class PropertiesBasedKinesisConfig extends PropertiesBasedConfig
{
  public PropertiesBasedKinesisConfig()
  {
    super(
        ImmutableSet.of(
            "kinesis.appName",
            "kinesis.initialStreamPosition",
            "kinesis.endpointUrl",
            "kinesis.streamName",
            "kinesis.regionName",
            "consumer.numThreads",
            "commit.periodMillis"
        )
    );
  }

  @Config("kinesis.appName")
  @Default("tranquility-kinesis")
  public abstract String getKinesisAppName();

  @Config("kinesis.streamName")
  public abstract String getKinesisStreamName();

  @Config("kinesis.regionName")
  @Default("us-west-1")
  public abstract String getKinesisRegionName();

  @Config("kinesis.initialStreamPosition")
  @Default("Latest")
  public abstract String getInitialStreamPosition();

  public InitialPositionInStream getInitialStreamPositionJava(){
    if(getInitialStreamPosition().equals("TRIM_HORIZON")){
      return InitialPositionInStream.TRIM_HORIZON;
    }else{
      return InitialPositionInStream.LATEST;
    }
  }

  @Config("consumer.numThreads")
  @Default("-1")
  public abstract Integer getConsumerNumThreads();

  @Config("commit.periodMillis")
  @Default("15000")
  public abstract Integer getCommitPeriodMillis();

  @Config("reportDropsAsExceptions")
  @Default("false")
  public abstract Boolean reportDropsAsExceptions();

  @Config("reportParseExceptions")
  @Default("false")
  public abstract Boolean reportParseExceptions();
}
