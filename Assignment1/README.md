<!-- In VS code, use ctrl + shift + v to see preview -->

<br />

# READEME

## How to run

<br />

If new dependencies are added, run the command: mvn clean package

mvn dependency:copy-dependencies

mvn package

java -cp target/Assignment1-1.0-SNAPSHOT.jar:target/dependency/\* Assignment1.LocalApplication input-sample-1.txt outputFileName.txt 500 terminate

<br />
<br />

**Runing with Amazon Web Services**

**We used instance of type T2_NANO with ami: "ami-00e95a9222311e8ed".**

<!-- dealete ami if publied  -->

<br />

## System Overview

<br />

![Assignment 1 System Architecture, saved in the Appendices folder ](./Appendices/DS_A1_.diagram.png)
[System Architecture Diagram](https://drive.google.com/file/d/1eUvCboxx64iXHZsguHVfUJEjFcMaDMJf/view?usp=sharing) &nbsp; &nbsp; [Diagram as photo](https://drive.google.com/file/d/1-_z2TBPwo3r0QdSevMOjwZ-znCDvqvay/view?usp=sharing)

### The system consisting of the following components:

#### 1. Local Application

#### 2. EC2 Manager Node

- Creates the workers nodes, control the data flow, splits the input into tasks, distributes them to Workers, and combines results.

#### 3. EC2 Worker Nodes

#### 4. S3 Storage

#### 5. SQS Queues

<br />

The system uses S3 as a common storage and SQS as the primary method for communication. The design enables the Manager to assign tasks to the Workers by sending messages to a queue. The Workers, during their free time, can then retrieve and process these tasks. This design is similar to the reactor server model and shares similar advantages.

The Manager only performs the simple task of defining the task and placing it in the queue, without needing to find a free worker or manage task distribution.
The Workers are not disturbed while working and can easily check if there are additional tasks to process.

<br />

## System Scalability

The system is built such that all the components work with a common data storage and communicated using common queues. As such, the components do not interact directly resulting The amount of communication is linear in the number of components. This design ensures that the Manager node does not need to communicate with a large number of nodes, making the design scalable.

Additionally, the system uses S3 storage and SQS, which can store an adjustable amount of data and messages.

The main weakness of the design is the Manager, which is always a single node. This weakness is mitigated because the Manager only performs a simple and fast task.

<br />

## System Persistence

The system is persistent and can handle the failure of of its components.

- If a worker node fails, it will no longer take new tasks. Additionally, due to the Amazon SQS visibility timeout process, the message will eventually become visible and another worker will take the task.
- Both S3 and SQS provide live backups and ensure durability of the data.

[Data Durability in Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DataDurability.html) &nbsp; &nbsp; [Amazon SQS Documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)

- If the manager fails, the data is still secure, but the process will only continue once a new manager node is manually created or the local application is re-run.

<br />

## System Decentralization

The system consists of multiple independent components, each operating on separate machines. These components communicate through shared storage (S3) and message queues (SQS), enabling them to function autonomously while exchanging data.

The system operates in parallel as tasks are processed concurrently by multiple Worker Nodes. The workers do not wait for others to finish and can independently retrieve additional tasks from the queue.

<!-- TODO, add reference to threads -->
