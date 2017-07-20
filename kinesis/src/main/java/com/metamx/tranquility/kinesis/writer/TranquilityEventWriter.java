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
package com.metamx.tranquility.kinesis.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.scala.net.curator.Disco;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.finagle.FinagleRegistry;
import com.metamx.tranquility.finagle.FinagleRegistryConfig;
import com.metamx.tranquility.kinesis.KinesisBeamUtils;
import com.metamx.tranquility.kinesis.model.MessageCounters;
import com.metamx.tranquility.kinesis.model.PropertiesBasedKinesisConfig;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import scala.runtime.BoxedUnit;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pushes events to Druid through Tranquility using the SimpleTranquilizerAdapter.
 */
public class TranquilityEventWriter
{
  private static final Logger log = new Logger(TranquilityEventWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final DataSourceConfig<PropertiesBasedKinesisConfig> dataSourceConfig;
  private final Tranquilizer<byte[]> tranquilizer;

  private final AtomicLong receivedCounter = new AtomicLong();
  private final AtomicLong sentCounter = new AtomicLong();
  private final AtomicLong droppedCounter = new AtomicLong();
  private final AtomicLong unparseableCounter = new AtomicLong();
  private final AtomicReference<Throwable> exception = new AtomicReference<>();
  private static final RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(1000, 500, 30000);


  public TranquilityEventWriter(
      DataSourceConfig<PropertiesBasedKinesisConfig> dataSourceConfig
  )
  {

    final int zkTimeout = Ints.checkedCast(
            dataSourceConfig.propertiesBasedConfig()
                    .zookeeperTimeout()
                    .toStandardDuration()
                    .getMillis()
    );

    final CuratorFramework curator = CuratorFrameworkFactory.builder()
            .connectString(
                    dataSourceConfig.propertiesBasedConfig()
                            .zookeeperConnect()
            )
            .connectionTimeoutMs(zkTimeout)
            .retryPolicy(RETRY_POLICY)
            .build();
    curator.start();


    FinagleRegistry finagleRegistry=  new FinagleRegistry(
            FinagleRegistryConfig.builder().build(),
            new Disco(curator, dataSourceConfig.propertiesBasedConfig())
    );

    this.dataSourceConfig = dataSourceConfig;
    this.tranquilizer = KinesisBeamUtils.createTranquilizer(
        dataSourceConfig,
        curator,
        finagleRegistry
    );
    this.tranquilizer.start();
  }

  public void send(byte[] message) throws InterruptedException
  {
    receivedCounter.incrementAndGet();
    tranquilizer.send(message).addEventListener(
        new FutureEventListener<BoxedUnit>()
        {
          @Override
          public void onSuccess(BoxedUnit value)
          {
            sentCounter.incrementAndGet();
          }

          @Override
          public void onFailure(Throwable cause)
          {
            if (cause instanceof MessageDroppedException) {
              droppedCounter.incrementAndGet();
              if (!dataSourceConfig.propertiesBasedConfig().reportDropsAsExceptions()) {
                return;
              }
            } else if (cause instanceof ParseException) {
              unparseableCounter.incrementAndGet();
              if (!dataSourceConfig.propertiesBasedConfig().reportParseExceptions()) {
                return;
              }
            }

            exception.compareAndSet(null, cause);
          }
        }
    );

    maybeThrow();
  }

  public void flush() throws InterruptedException
  {
    tranquilizer.flush();
    maybeThrow();
  }

  public void stop()
  {
    try {
      tranquilizer.stop();
    }
    catch (IllegalStateException e) {
      log.info(e, "Exception while stopping Tranquility");
    }
  }

  public MessageCounters getMessageCounters()
  {
    return new MessageCounters(
        receivedCounter.get(),
        sentCounter.get(),
        droppedCounter.get(),
        unparseableCounter.get()
    );
  }

  private void maybeThrow()
  {
    final Throwable e = exception.get();
    if (e != null) {
      throw Throwables.propagate(e);
    }
  }
}
