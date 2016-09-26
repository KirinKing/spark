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
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  // Mapping from broadcast id to executor broadcast rdds.
  private val executorBroadcastRdds = new HashMap[Long, RDD[Any]]

  private val alreadyRecove = new mutable.HashSet[(Int, Int)]

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
    driverEndpoint.send(StopBroadcastManager)
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
      id: Long,
      isExecutorSide: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, id, isExecutorSide)
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }

  def addBroadcastRdd(id: Long, rdd: RDD[Any]): Unit = {
    executorBroadcastRdds.put(id, rdd)
  }


  def recoverBlocks(id: Long, stageId: Int, stageAttemptId: Int): Boolean = {
    driverEndpoint.askWithRetry[Boolean](RecoverBroadcast(id, stageId, stageAttemptId))
  }

  // This endpoint is used only for RPC
  private[spark] class BroadcastManagerEndpoint(
      override val rpcEnv: RpcEnv) extends RpcEndpoint with Logging {

      override def receive: PartialFunction[Any, Unit] = {
        case StopBroadcastManager =>
          logInfo("OutputCommitCoordinator stopped!")
          stop()
      }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RecoverBroadcast(id, stageId, stageAttemptId) =>
        // only allowed recover once for a stage attempt
        if (!alreadyRecove.contains((stageId, stageAttemptId))) {
          alreadyRecove.add((stageId, stageAttemptId))
          executorBroadcastRdds.get(id).reBroadcast(id)
        }
        // clear the alreadyRecove to avoid increase infinitly
        if (alreadyRecove.size > 10000) alreadyRecove.clear()
        context.reply(true)
    }
  }
}

object BroadcastManager {
  val ENDPOINT_NAME = "BroadcastManager"
}
case object StopBroadcastManager
case class RecoverBroadcast(id: Long, stageId: Int, stageAttemptId: Int)
