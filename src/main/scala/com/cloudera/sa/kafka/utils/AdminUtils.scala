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

import joptsimple.OptionParser

object AdminUtils {

  def isConsumerGroupActive(zkUtils: ZkUtils, group: String) = {
    zkUtils.getConsumersInGroup(group).nonEmpty
  }

  /**
    * Delete the whole directory of the given consumer group if the group is inactive.
    *
    * @param zkUtils Zookeeper utilities
    * @param group Consumer group
    * @return whether or not we deleted the consumer group information
    */
  def deleteConsumerGroupInZK(zkUtils: ZkUtils, group: String) = {
    if (!isConsumerGroupActive(zkUtils, group)) {
      val dir = new ZKGroupDirs(group)
      zkUtils.deletePathRecursive(dir.consumerGroupDir)
      true
    }
    else false
  }

  /**
    * Print usage and exit
    * Note: Extracted from kafka.utils.CommandLineUtils.
    */
  def printUsageAndDie(parser: OptionParser, message: String): Nothing = {
    System.err.println(message)
    parser.printHelpOn(System.err)
    sys.exit(1)
  }

}
