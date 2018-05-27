/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off
package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object Dynamic {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("CustomReceiver")
      .set("dynamic", "true")
      .setMaster("spark://127.0.0.1:7077")
//      .setMaster("local")
      .set("spark.jars", "/Users/max/IdeaProjects/spark/examples/target/original-spark-examples_2.11-2.3.2-SNAPSHOT.jar")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val ssc = new StreamingContext(sparkConf, Seconds(1))

    ssc.receiverStream(new DynamicReceiver())
      .flatMap { line =>
        Thread.sleep(Random.nextInt(1200))
        line.split(" ")
      }
      .map(x => (x, 1))
      .reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
