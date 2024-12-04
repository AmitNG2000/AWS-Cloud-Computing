package assignment1;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;

public class AWS {

    public final static String IMAGE_AMI = "ami-08902199a8aa0bc09"; // Default AMI ID

    public static final String LOCAL_MANAGER_QUEUE_NAME = "LocalManagerQueue";
    public static final String MANAGER_TO_WORKER_QUEUE_NAME = "LocalToManagerQueue";
    public static final String WORKER_TO_MANAGER_QUEUE_NAME = "ManagerToLocalQueue";
    public static final String bucketName = "default-bucket";

    public final Region region = Region.US_WEST_2; // Default AWS Region
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    private static AWS instance = null;

    public static String ami = "ami-00e95a9222311e8ed";
    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    // Private constructor for Singleton
    private AWS() {
        s3 = S3Client.builder().region(region).build();
        sqs = SqsClient.builder().region(region).build();
        ec2 = Ec2Client.builder().region(region).build();
    }

    // Thread-safe Singleton implementation
    public static synchronized AWS getAWSInstance() {
        if (instance == null) {
            instance = new AWS();
        }
        return instance;
    }

    // // Set the bucket name dynamically
    // public void setBucketName(String bucketName) {
    // this.bucketName = bucketName;
    // }

    ////////////////////////////////////////// EC2

    // Given by Meni to create EC2
    public String createEC2(String script, String tagName, int numberOfInstances) {
        numberOfInstances = Math.min(9, numberOfInstances); // so not to exceed the limit for students
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_NANO)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    // // Create EC2 with AMI
    // public void runInstanceFromAMI(String ami) {
    // Tag tag = Tag.builder().key("Name").value("Manager").build();
    // TagSpecification tagSpecification = TagSpecification.builder()
    // .resourceType(ResourceType.INSTANCE)
    // .tags(tag)
    // .build();

    // RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
    // .imageId(ami)
    // .instanceType(InstanceType.T2_MICRO)
    // .minCount(1)
    // .maxCount(1)
    // .tagSpecifications(tagSpecification)
    // .build();

    // try {
    // RunInstancesResponse response = ec2.runInstances(runInstancesRequest);
    // response.instances()
    // .forEach(instance -> System.out.println("Launched instance with ID: " +
    // instance.instanceId()));
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }

    // // Terminate EC2 instance
    // public void terminateInstance(String instanceId) {
    // TerminateInstancesRequest terminateRequest =
    // TerminateInstancesRequest.builder()
    // .instanceIds(instanceId)
    // .build();

    // try {
    // ec2.terminateInstances(terminateRequest);
    // System.out.println("Terminated instance: " + instanceId);
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }

    // Get all EC2 instances active
    public List<Instance> getAllInstances() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        try {
            DescribeInstancesResponse response = ec2.describeInstances(request);
            return response.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    // Get all EC2 instances with a specific tag
    public List<Instance> getInstancesByTag(String tagValue) {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("tag:Name")
                                .values(tagValue)
                                .build())
                .build();

        try {
            DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);
            return describeInstancesResponse.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    ////////////////////////////////////////////// S3

    // Create a bucket
    public void createBucket(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            System.out.println("Bucket created: " + bucketName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Delete bucket
    public void deleteBucket(String bucketName) {
        try {
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
            System.out.println("Bucket deleted: " + bucketName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Upload a file to S3
    public String uploadFileToS3(String keyPath, File file) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyPath)
                    .build();
            s3.putObject(request, file.toPath());
            System.out.println("Uploaded file: " + file.getName());
            return "s3://" + bucketName + "/" + keyPath;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Download a file from S3
    public void downloadFileFromS3(String keyPath, File outputFile) {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyPath)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(request);
            byte[] data = objectBytes.asByteArray();
            try (OutputStream os = new FileOutputStream(outputFile)) {
                os.write(data);
            }
            System.out.println("Downloaded file: " + outputFile.getPath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    ////////////////////////////////////////////// SQS

    // Create a queue if it doesn't exist
    public String createQueue(String queueName) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse response = sqs.createQueue(request);
            String queueUrl = response.queueUrl();
            System.out.println("Queue created: " + queueName + " URL: " + queueUrl);
            return queueUrl;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Get Queue Url to get the SQS
    public String getQueueUrl(String queueName) {
        try {
            GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            GetQueueUrlResponse response = sqs.getQueueUrl(request);
            return response.queueUrl();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Send a message to SQS
    public void sendMessageToSQS(String queueName, String s3Url) {
        try {
            String queueUrl = getQueueUrl(queueName);
            if (queueUrl == null) {
                queueUrl = createQueue(queueName);
            }

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(s3Url)
                    .build();

            sqs.sendMessage(sendMsgRequest);
            System.out.println("Message sent to SQS queue '" + queueName + "': " + s3Url);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Receive messages from SQS
    public List<Message> receiveMessagesFromSQS(String queueUrl) {
        try {
            ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(10)
                    .build();

            ReceiveMessageResponse response = sqs.receiveMessage(request);
            return response.messages();
        } catch (Exception e) {
            e.printStackTrace();
            return List.of(); // Return an empty list on failure
        }
    }

    // Delete message from SQS
    public void deleteMessageFromSQS(String queueName, String receiptHandle) {
        try {
            String queueUrl = getQueueUrl(queueName);
            if (queueUrl == null) {
                System.err.println("[ERROR] Queue does not exist.");
                return;
            }

            DeleteMessageRequest deleteMsgRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();

            sqs.deleteMessage(deleteMsgRequest);
            System.out.println("Message deleted from SQS queue '" + queueName + "'.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Delete SQS queue
    public void deleteQueue(String queueUrl) {
        try {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            System.out.println("Queue deleted: " + queueUrl);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Get SQS Size
    public int getQueueSize(String queueUrl) {
        try {
            GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build();

            GetQueueAttributesResponse response = sqs.getQueueAttributes(request);
            String messages = response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
            return Integer.parseInt(messages);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    // Get type of Tag name
    public static enum Node {
        LOCAL_APPLICATION,
        MANAGER,
        WORKER
    }

    // s3
    public void createBucketIfNotExists() {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void terminateInstance(String instanceId) {

        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        TerminateInstancesResponse terminateResponse = ec2.terminateInstances(terminateRequest);

        System.out.println("Terminated instance: " + terminateResponse.terminatingInstances());

    }
}
