/**
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

package com.cloudera.sa.kafka.utils

import kafka.utils.ZkUtils._
import kafka.utils.{Logging, ZKStringSerializer}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.{ZkClient, ZkConnection}

import scala.collection._

object ZkUtils {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val DeleteTopicsPath = "/admin/delete_topics"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"
  val BrokerSequenceIdPath = "/brokers/seqid"
  val IsrChangeNotificationPath = "/isr_change_notification"
  val EntityConfigPath = "/config"
  val EntityConfigChangesPath = "/config/changes"

  def apply(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int, isZkSecurityEnabled: Boolean): ZkUtils = {
    val (zkClient, zkConnection) = createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)
    new ZkUtils(zkClient, zkConnection, isZkSecurityEnabled)
  }

  def createZkClientAndConnection(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int): (ZkClient, ZkConnection) = {
    val zkConnection = new ZkConnection(zkUrl, sessionTimeout)
    val zkClient = new ZkClient(zkConnection, connectionTimeout, ZKStringSerializer)
    (zkClient, zkConnection)
  }
}

class ZkUtils(val zkClient: ZkClient,
              val zkConnection: ZkConnection,
              val isSecure: Boolean) extends Logging {
  // These are persistent ZK paths that should exist on kafka broker startup.
  val persistentZkPaths       = Seq(ConsumersPath, BrokerIdsPath, BrokerTopicsPath, DeleteTopicsPath)
  val securePersistentZkPaths = Seq(BrokerIdsPath, BrokerTopicsPath, DeleteTopicsPath)

  def deletePathRecursive(path: String) {
    try {
      zkClient.deleteRecursive(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2: Throwable => throw e2
    }
  }

  def getChildren(path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    zkClient.getChildren(path)
  }

  def getChildrenParentMayNotExist(path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    try {
      zkClient.getChildren(path)
    } catch {
      case e: ZkNoNodeException => Nil
      case e2: Throwable => throw e2
    }
  }

  def getConsumersInGroup(group: String): Seq[String] = {
    val dirs = new ZKGroupDirs(group)
    getChildren(dirs.consumerRegistryDir)
  }

  def getAllTopics(): Seq[String] = {
    val topics = getChildrenParentMayNotExist(BrokerTopicsPath)
    if(topics == null)
      Seq.empty[String]
    else
      topics
  }

  def getConsumerGroups() = {
    getChildren(ConsumersPath)
  }

  def close() {
    if (zkClient != null) {
      zkClient.close()
    }
  }
}

class ZKGroupDirs(val group: String) {
  def consumerDir = ConsumersPath
  def consumerGroupDir = consumerDir + "/" + group
  def consumerRegistryDir = consumerGroupDir + "/ids"
  def consumerGroupOffsetsDir = consumerGroupDir + "/offsets"
  def consumerGroupOwnersDir = consumerGroupDir + "/owners"
}

class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
  def consumerOffsetDir = consumerGroupOffsetsDir + "/" + topic
  def consumerOwnerDir  = consumerGroupOwnersDir  + "/" + topic
}
