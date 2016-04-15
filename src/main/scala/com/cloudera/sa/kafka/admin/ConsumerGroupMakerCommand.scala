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

package com.cloudera.sa.kafka.admin

import java.util.Properties

import com.cloudera.sa.kafka.utils.{AdminUtils, ZkUtils}
import joptsimple.{OptionException, OptionParser, OptionSpec}
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}
import kafka.utils.{CommandLineUtils, Utils}
import org.I0Itec.zkclient.exception.ZkNoNodeException

object ConsumerGroupMakerCommand {

  private var opts: ConsumerGroupMakerOptions = null

  def main(args: Array[String]): Unit = {
    opts = new ConsumerGroupMakerOptions(args)
    opts.checkArgs()

    val consumerGroupService = new ZkConsumerGroupService(opts)
    try {
      if      (opts.options.has(opts.listOpt))   consumerGroupService.list()
      else if (opts.options.has(opts.deleteOpt)) consumerGroupService.deleteForGroup()
      else if (opts.options.has(opts.createOpt)) consumerGroupService.createGroup()
    } catch {
      case e: Throwable =>
        println("Error while executing consumer group command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      consumerGroupService.close()
    }
  }

  class ZkConsumerGroupService(val opts: ConsumerGroupMakerOptions) {

    private val zkUtils = ZkUtils(opts.options.valueOf(opts.zkConnectOpt), 30000, 30000, false)

    def list(): Unit = {
      zkUtils.getConsumerGroups().foreach(println)
    }

    def close(): Unit = {
      zkUtils.close()
    }

    def deleteForGroup(): Unit = {
      val group = opts.options.valueOf(opts.groupOpt)
      try {
        if (AdminUtils.deleteConsumerGroupInZK(zkUtils, group))
          println("Deleted all consumer group information for group %s in zookeeper.".format(group))
        else
          println("Delete for group %s failed because its consumers are still active.".format(group))
      }
      catch {
        case e: ZkNoNodeException =>
          println("Delete for group %s failed because group does not exist.".format(group))
      }
    }

    def createGroup(): Unit = {
      // See Section 3.2 Consumer Configs of http://kafka.apache.org/082/configuration.html
      val props = new Properties()
      props.put("group.id",                        opts.options.valueOf(opts.groupOpt))
      props.put("zookeeper.connect",               opts.options.valueOf(opts.zkConnectOpt))
      props.put("zookeeper.connection.timeout.ms", "1000")
      props.put("socket.timeout.ms",               "1000")

      val topic = opts.options.valueOf(opts.topicOpt)
      var consumer: ConsumerConnector = null;
      try {
        consumer = Consumer.create(new ConsumerConfig(props))
        consumer.createMessageStreams(Map(topic -> 1)).get(topic)
      } catch {
        case e: org.I0Itec.zkclient.exception.ZkTimeoutException => {
          println(e.getMessage)
          sys.exit(1)
        }
      }

      try { Thread.sleep(500); } catch { case e: InterruptedException => {} }
      consumer.shutdown()
    }
  }

  class ConsumerGroupMakerOptions(args: Array[String]) {
    val ZkConnectDoc = "REQUIRED: Connection string for the zookeeper connection in the form host:port. Multiple URLS can be given to allow fail-over."
    val GroupDoc     = "Consumer group we wish to act on."
    val TopicDoc     = "Topic whose consumer group information should be deleted."
    val ListDoc      = "List all consumer groups."
    val nl           = System.getProperty("line.separator")
    val DeleteDoc    = "Pass in group to delete."
    val CreateDoc    = "Create consumer group."

    val parser       = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", ZkConnectDoc).withRequiredArg.describedAs("urls"          ).ofType(classOf[String])
    val groupOpt     = parser.accepts("group",     GroupDoc    ).withRequiredArg.describedAs("consumer group").ofType(classOf[String])
    val topicOpt     = parser.accepts("topic",     TopicDoc    ).withRequiredArg.describedAs("topic"         ).ofType(classOf[String])
    val listOpt      = parser.accepts("list",      ListDoc)
    val deleteOpt    = parser.accepts("delete",    DeleteDoc)
    val createOpt    = parser.accepts("create",    CreateDoc)
    val options      = tryParse(parser, args)

    val allConsumerGroupLevelOpts: Set[OptionSpec[_]] = Set(listOpt, deleteOpt, createOpt)

    def tryParse(parser: OptionParser, args: Array[String]) = {
      try {
        parser.parse(args : _*)
      } catch {
        case e: OptionException => {
          AdminUtils.printUsageAndDie(parser, e.getMessage)
          null
        }
      }
    }

    def checkArgs() {
      if (args.length == 0)
        AdminUtils.printUsageAndDie(parser, "List all consumer groups; Create or delete consumer groups.")

      val actions = Seq(listOpt, deleteOpt, createOpt).count(options.has _)
      if (actions != 1)
        AdminUtils.printUsageAndDie(parser, "Command must include exactly one action: --list, --delete, --create")

      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)

      if (options.has(deleteOpt) && !options.has(groupOpt))
        AdminUtils.printUsageAndDie(parser, "Option %s requires %s".format(deleteOpt, groupOpt))
      if (options.has(createOpt) && (!options.has(groupOpt) || !options.has(topicOpt))) {
        AdminUtils.printUsageAndDie(parser, "Option %s requires %s and %s".format(createOpt, groupOpt, topicOpt))
      }

      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, allConsumerGroupLevelOpts - deleteOpt - createOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, allConsumerGroupLevelOpts - deleteOpt - createOpt)
    }
  }

}
