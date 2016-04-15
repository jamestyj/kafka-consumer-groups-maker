# kafka-consumer-groups-maker

## Introduction

Command line tool to manage (e.g. list, create, and delete) Kafka [consumer
groups](http://kafka.apache.org/documentation.html#intro_consumers).

Kafka consumer groups are typically created by the consumers, however some 3rd
party consumers don't have the ability to do so. There currently isn't any
command line tool that can create consumer groups (particularly for Kafka
0.8.2), so let's write one.

Much of the code are back ports of the new
[kafka-consumer-groups](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/admin/ConsumerGroupCommand.scala)
command in Kafka 0.9 (for the consumer groups list and delete functions).

## Usage

### Overview

    $ kafka-consumer-groups-maker
    List all consumer groups; Create or delete consumer groups.
    Option                    Description
    ------                    -----------
    --create                  Create consumer group.
    --delete                  Pass in group to delete.
    --group <consumer group>  Consumer group we wish to act on.
    --list                    List all consumer groups.
    --topic <topic>           Topic whose consumer group information
                                should be deleted.
    --zookeeper <urls>        REQUIRED: Connection string for the
                                zookeeper connection in the form
                                host:port. Multiple URLS can be
                                given to allow fail-over.

## Installation

1. Build the JAR locally by running:

        sbt assembly

2. Place the resulting JAR in the following path:

        /opt/cloudera/parcels/KAFKA-*/lib/kafka/libs

3. Copy [kafka-consumer-groups-maker](bin/kafka-consumer-groups-maker) to the
  following path:

        /opt/cloudera/parcels/KAFKA-*/bin/

4. Copy [kafka-consumer-groups-maker.sh](bin/kafka-consumer-groups-maker.sh) to
   the following path:

        /opt/cloudera/parcels/KAFKA-*/lib/kafka/bin/

5. Create a symlink by running:

        cd /usr/bin
        sudo ln -s /opt/cloudera/parcels/KAFKA-*/bin/kafka-consumer-groups-maker
