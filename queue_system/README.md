# Queue systems


## Introduction

When you start having an important amount of data to process you will need to use a queue system to process the data in the background.

Imagine, you work for Delhaize. Each time a customer buys something in one of the shops, some data are sent to the server. You need to pre-process those data and store them for later use. You can't do that in real time because you will have a lot of data to process and you will need to do some heavy computation. So you need to use a queue system to process the data as soon as possible. You know that the shops are closed at night, so you can catch up on your delays during that time.


## What is a queue system?

A queue system is a system that will store data and process them in the background. It's a very common system in the industry. It's used in a lot of different contexts. 

For example, when you send an email, the email is stored in a queue system and processed by a worker. The worker will send the email to the recipient. If the worker is not available, the email will be stored in the queue system and processed as soon as the worker is available.

It's the same for Discord, Linkedin, and Facebook,... They all use a queue system to process data in the background.

Here is a [2 minutes video](https://www.youtube.com/watch?v=uvb00oaa3k8) explaining what is a queue system with nice visuals!.


## How does it work?

A queue system is composed of two parts: the queue and the worker.

### The queue
The queue is a storage system that will store messages (containing the data). The worker is a process that will process the message. 

### The worker
The worker will take a message from the queue and process it. When the worker is done, it will take another message from the queue and so on, until the queue is empty.

If the message can't be processed (an error occurred), the message will be put back in the queue and processed later or can be sent to a dead-letter queue, a queue containing all the messages that can't be processed. It's particularly useful to debug your application.


## How to get started?

There are a lot of queue systems available. The most popular are RabbitMQ, Redis, Kafka, SQS, Beanstalkd, and IronMQ, ...

In this tutorial, we will use Kafka. It's a very popular queue system. It's very easy to use and it's very powerful. Kafka is one of the most popular queue systems in the industry.

Setup Kafka can be a bit tricky. It's not the purpose of this tutorial. So we will use a docker image to set up Kafka.

We will use [this image](https://hub.docker.com/r/bitnami/kafka). It's a very popular image. It's very easy to use and very well documented.

But let's leave that for the practical part.

## Some vocabulary

Before starting, we need to define some vocabulary.

### Topic

A topic is a **category of data**. 

For example, you can have a topic for the data related to food products, another topic for the data related to home products, etc.

### Partition

A partition is a **part of a topic**. 

For example, you can have a topic with 3 partitions. Each partition will contain the kind of data (food product) but we will have one partition per store, 1 for Delhaize Brussels, 1 for Charleroi, and one for Antwerp.


### Offset

An offset is a number that represents the **position of a message in a partition**. For example, if you have a topic with 3 partitions, the first message of the first partition will have an offset of 0, the second message of the first partition will have an offset of 1, etc.

### Consumer

A consumer Aka a worker is a **process that will process the message** of a topic. It will take a message from the topic and process it. When it's done, it will take another message and so on, until the topic is empty.

### Consumer group

A consumer group is a **group of consumers**. 

For example, you can have a consumer group with 3 consumers. Each consumer will process the data on the topic.


### Producer

A producer is a process that will **send a message to a topic**. 

### Broker

A broker is a process that will manage the topics. It will **store the messages** of the topics and **send it** to the consumers.

It can seem hard but it isn't. It's just a lot of vocabulary. But don't worry, we will see how to use it in the practical part.

### Zookeeper

Zookeeper is a process that will manage the brokers. It will **store the metadata** of the brokers. It will also **manage the consumer groups**.

[More on that topic here](https://www.cloudkarafka.com/blog/cloudkarafka-what-is-zookeeper.html)


## Schema
Here is a schema to make things more clear.

![schema](./assets/kafka_schema.png)
*[Source](https://www.cloudkarafka.com/blog/part1-kafka-for-beginners-what-is-apache-kafka.html)*


## Next steps
Ready to start? Let's go to the [practical part](./1.simple_practice.md)!

![zookeeper meme](https://media1.giphy.com/media/3o6ZsUUijfVikNf8Eo/giphy.gif?cid=ecf05e47hy7ujw2fzogr6bzmxd36b9rkl8soyv44h4pg4kwx&rid=giphy.gif&ct=g)
