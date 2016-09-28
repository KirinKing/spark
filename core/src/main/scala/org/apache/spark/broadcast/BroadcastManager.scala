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

package org.apache.spark.broadcast

import java.util.HashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  // Mapping from broadcast id to ReBroadcast of executor broadcast.
  private val executorBroadcasts = new HashMap[Long, Any]

  // a stage attempt that have already reBroadcast a rdd
  private val reBroadcasted = new mutable.HashSet[(Long, Int, Int)]

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null
  var driverEndpoint: RpcEndpointRef = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
    driverEndpoint = null
  }

  private val nextBroadcastId = new AtomicLong(0)

  // Called from driver to create new broadcast id
  def newBroadcastId: Long = nextBroadcastId.getAndIncrement()

  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  // Called from executor to create broadcast with specified id
  def newBroadcast[T: ClassTag](
      value_ : T,
      isLocal: Boolean,
      id: Long
      ): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, id)
  }

  // Called from driver to create broadcast with specified id
  def newBroadcast[T: ClassTag, U: ClassTag](
      value_ : T,
      isLocal: Boolean,
      id: Long,
      isExecutorSide: Boolean,
      transFunc: TransFunc[U, T]): Broadcast[T] = {
    broadcastFactory.newBroadcast[T, U](value_, isLocal, id, isExecutorSide, transFunc)
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }

  def registerReBroadcast[T: ClassTag, U: ClassTag](
      id: Long,
      reBroadcast: ReBroadcast[T, U]): Unit = {
    executorBroadcasts.put(id, reBroadcast)
  }

  def reBroadcast(id: Long, stageId: Int, stageAttemptId: Int): Boolean = {
    driverEndpoint.askWithRetry[Boolean](RecoverBroadcast(id, stageId, stageAttemptId))
  }

  // This endpoint is used only for RPC
  private[spark] class BroadcastManagerEndpoint(
      override val rpcEnv: RpcEnv) extends RpcEndpoint with Logging {

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RecoverBroadcast(id, stageId, stageAttemptId) =>
        // only allowed recover once for a stage attempt
        if (!reBroadcasted.contains((id, stageId, stageAttemptId))) {
          reBroadcasted.add((id, stageId, stageAttemptId))
          executorBroadcasts.get(id) match {
            case re: ReBroadcast[_, _] =>
              re.reBroadcast(id)
          }
        }
        // clear the reBroadcasted to avoid increase infinitly
        if (reBroadcasted.size > 10000) reBroadcasted.clear()
        context.reply(true)
    }
  }
}

private[spark] case class ReBroadcast[T: ClassTag, U: ClassTag](
    rdd: RDD[T],
    transFunc: TransFunc[T, U]) extends Logging{
  def reBroadcast(id: Long): Unit = {
    // todo:
    val bc = rdd.coalesce(1).mapPartitions { iter =>
      Iterator(SparkEnv.get.broadcastManager.newBroadcast(
        transFunc.transform(iter.toArray), false, id))
    }.collect().head
    logWarning("Rebroadcast " + bc.id)
  }
}


/**
 * Marker trait to identify the shape in which tuples are broadcasted. User must passed one
 * TransFunc when executor side broadcast(rdd.broadcast), typical examples of this are
 * identity (tuples remain unchanged) or hashed (tuples are converted into some hash index).
 */
trait TransFunc[T, U] extends Serializable {
  def transform(rows: Array[T]): U
}

object BroadcastManager {
  val ENDPOINT_NAME = "BroadcastManager"
}
case class RecoverBroadcast(id: Long, stageId: Int, stageAttemptId: Int)
