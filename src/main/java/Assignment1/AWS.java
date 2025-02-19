package Assignment1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class AWS {

    public static final String LOCAL_MANAGER_QUEUE_NAME = "LocalManagerQueue";
    public static final String MANAGER_TO_WORKER_QUEUE_NAME = "LocalToManagerQueue";
    public static final String WORKER_TO_MANAGER_QUEUE_NAME = "ManagerToLocalQueue"; //#TODO CHANGE THE NAME
    public static final String bucketName = "localapplicationbucket";
    public static int activeWorkersCaution = 0;

    public final Region region = Region.US_EAST_1; // Default AWS Region
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

    public static boolean debug_messages = true;

    public static void debug(String text) {
        if (debug_messages)
            System.out.println("[DEBUG] " + text);
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

    public List<Instance> getRunningInstancesByTag(String tagValue) {
        // Define a filter for the "tag:Name" and another for instance state
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("tag:Name")
                                .values(tagValue)
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running") // Only fetch running instances
                                .build())
                .build();
    
        try {
            // Fetch the instances matching the filters
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

    public void createBucketIfDoesntExists() {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .build()); // No locationConstraint for US_EAST_1
    
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            AWS.debug("Bucket created successfully or already exists.");
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
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
            return keyPath;
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

    public boolean doesFileExistInS3(String keyPath) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyPath)
                    .build();
    
            s3.headObject(headObjectRequest);
            return true; 
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            System.err.println("S3 error: " + e.getMessage());
            return false;
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
            debug("Queue created: " + queueName);
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
        } catch (QueueDoesNotExistException e) {
            debug("Queue " + queueName + " does not exist.");
            return null;
        } catch (SqsException e) {
            debug("SQS error occurred: " + e.getMessage());
            return null;
        } catch (Exception e) {
            debug("Unexpected error: " + e.getMessage());
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
            debug("Message sent to SQS queue '" + queueName + "': " + s3Url);
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
            debug("Message deleted from SQS queue '" + queueName + "'.");
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
        // Build the request to get queue attributes
        GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                .build();

        // Fetch queue attributes
        GetQueueAttributesResponse response = sqs.getQueueAttributes(request);
        String messages = response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);

        // Return the number of messages as an integer
        return Integer.parseInt(messages);
    } catch (QueueDoesNotExistException e) {
        // Handle the case where the queue does not exist
        System.err.println("[WARNING] Queue does not exist: " + queueUrl);
        return 0;
    } catch (Exception e) {
        // Handle other exceptions
        System.err.println("[ERROR] Failed to get queue size for: " + queueUrl);
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
    

    public void terminateInstance(String instanceId) {

        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        TerminateInstancesResponse terminateResponse = ec2.terminateInstances(terminateRequest);

        System.out.println("Terminated instance: " + terminateResponse.terminatingInstances());

    }

    public String getInstanceId() {
        String INSTANCE_METADATA_URL = "http://169.254.169.254/latest/meta-data/instance-id";
        try {
            URL url = new URL(INSTANCE_METADATA_URL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    return reader.readLine();
                }
            } else {
                throw new RuntimeException("Failed to fetch instance ID. HTTP response code: " + responseCode);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error fetching instance ID: " + e.getMessage(), e);
    }
}
}
