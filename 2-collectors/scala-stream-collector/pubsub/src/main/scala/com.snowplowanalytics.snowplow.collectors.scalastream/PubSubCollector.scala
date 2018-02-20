/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream

import model._
import sinks.PubSubSink

object PubSubCollector extends App with Collector {
  val (collectorConf, akkaConf) = parseConfig(args)

  val sinks = {
    val goodStream = collectorConf.streams.good
    val badStream = collectorConf.streams.bad
    val bufferConf = collectorConf.streams.buffer
    val (good, bad) = collectorConf.streams.sink match {
      case pc: PubSub =>
        (new PubSubSink(pc, bufferConf, goodStream), new PubSubSink(pc, bufferConf, badStream))
      case _ => throw new IllegalArgumentException("Configured sink is not PubSub")
    }
    CollectorSinks(good, bad)
  }

  run(collectorConf, akkaConf, sinks)
}