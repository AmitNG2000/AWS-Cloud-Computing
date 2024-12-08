package Assignment1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.sqs.model.Message;

/*
 * The Manager
 * 1. Downloads the input file from S3.
 * 2. Creates an SQS message for each URL in the input file together with the operation that should be performed on it.
 * 3. Checks the SQS message count and starts Worker processes (nodes) accordingly.
 * 4. (Taken from System Summary) Manager reads all Workers' messages from SQS and creates one summary file, once all URLs in the input file have been processed.
 * 5. (Taken from System Summary) Manager uploads the summary file to S3.
 * 6. (Taken from System Summary) Manager posts an SQS message about the summary file
 * 7. If the message is a termination message, then the manager handle termination as described.
 */

public class Manager {

    public static void main(String[] args) {
        AWS aws = AWS.getAWSInstance();
        // 1.
        String s3FileKey = receiveInputMessage(aws);
        File inputFile = downloadInputFromS3(aws, s3FileKey);
        // 2.
        //processInputFileToSQS(aws, inputFile);

        List<String> results = getAllWorkersMessages(aws);
        //4
        File summaryFile = createSummaryFile(results);
        // 5. + 6.
        uploadFileToS3AndSQS(aws, summaryFile);
        
        // Start a thread to handle worker messages continuously
        /* 
        Thread workerHandlerThread = new Thread(() -> {
            try {
                while (true) {
                    // 3.
                    handleWorkers(aws, Integer.parseInt(args[2]));
                    List<String> results = getAllWorkersMessages(aws);
                    // 4.
                    //File summaryFile = createSummaryFile(results);
                    // 5. + 6.
                    //uploadFileToS3AndSQS(aws, summaryFile);

                    // #TODO 7.

                    Thread.sleep(1000 * 1000); // Sleep for 1 seconds
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Exception in worker handling thread: " + e.getMessage());
                e.printStackTrace();
            }
        });

        workerHandlerThread.start(); // Start the thread
        */
        
    }

    // Gets the message of the location of the given input file.
    private static String receiveInputMessage(AWS aws) {
        String queueUrl = aws.getQueueUrl(AWS.LOCAL_MANAGER_QUEUE_NAME);
        List<Message> messages = aws.receiveMessagesFromSQS(queueUrl);

        if (messages.isEmpty()) {
            System.out.println("No messages in the queue.");
            return "";
        }

        Message message = messages.get(0);
        String s3FileKey = message.body();
        System.out.println("Received message from SQS with S3 file key: " + s3FileKey);

        return s3FileKey;
    }

    // 1. Downloads the input file from S3.
    private static File downloadInputFromS3(AWS aws, String s3FileKey) {
        File outputFile = new File(new File(s3FileKey).getName());
        aws.downloadFileFromS3(s3FileKey, outputFile);
        System.out.println("Input file downloaded and ready for processing: " + outputFile.getPath());
        return outputFile;
    }

    // 2. Creates an SQS message for each URL in the input file together with the
    // operation that should be performed on it.
    private static void processInputFileToSQS(AWS aws, File outputFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
            String line;
            AWS.debug("Starting to processInputFile...");
            AWS.debug_messages = false;
            int temp = 0; //#TODO change this, this takes too long
            while ((line = reader.readLine()) != null && temp < 15) {
                temp ++;

                if (!line.trim().isEmpty() && line.contains("\t")) {
                    aws.sendMessageToSQS(AWS.MANAGER_TO_WORKER_QUEUE_NAME, line.trim());
                    //AWS.debug("Message sent to worker queue: " + line);
                } else {
                    System.err.println("Skipping invalid line: " + line);
                }
            }
            AWS.debug_messages = true;
            AWS.debug("Finished processing InputFile.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 3. Checks the SQS message count and starts Worker processes (nodes)
    // accordingly.
    private static void handleWorkers(AWS aws, int n) {
        String script = 
    "#!/bin/bash\n" +
    "sudo apt update -y\n" +
    "sudo apt install -y default-jdk maven awscli\n" +
    "mkdir -p /home/ubuntu/worker\n" +
    "cd /home/ubuntu/worker\n" +
    "aws s3 cp s3://myjarsbucket/Worker.jar /home/ubuntu/worker/Worker.jar\n" +
    "aws s3 cp s3://myjarsbucket/pom.xml /home/ubuntu/worker/pom.xml\n" +
    "mvn dependency:copy-dependencies\n" +
    "java -cp Worker.jar:dependency/* Assignment1.Worker input-sample-1.txt outputFileName.txt 500 terminateLocaaws > /home/ubuntu/worker/worker.log 2>&1 &\n";

    String script2 = "#!/bin/bash\n" +
                "\n" +
                "# Set AWS credentials as environment variables\n" +
                "export AWS_ACCESS_KEY_ID=\"ASIAVA66XFVHU3SKH327\"\n" +
                "export AWS_SECRET_ACCESS_KEY=\"Whp4TPhDQvRiSdVbZgyXVq5XZHR60Yw1flWQHlDi\"\n" +
                "export AWS_SESSION_TOKEN=\"IQoJb3JpZ2luX2VjEKH//////////wEaCXVzLXdlc3QtMiJHMEUCIQC0hZp2ZkoljTU7iFw5dK478tSrx4ihWIHYowKaRDmeAgIgGZuF5hlrw5GLE1VD4xRJJM29nylxMNfzT3bXI6PgdcgqsQIIWhABGgwzNDU2NzUwODMwODciDGyhMm7fDNQIdwHKTiqOAiXR51qb74YcklkER/pMfg3b6mH2JChHXDVRxhWuMssISSGm5ofpeO4c2GmDJsYCoMsrdCPyrHDE624VNdO8CrhtRTsmLwqQ98Nt8zdTd7ez5X6TeEbcDKjrNVVk8kmk2PJxx2bON7PSCkVT6oZfOI9123/ErKHqg9eqpVFm6Vbf/Pn4MjNGqJUw4jM4cKJxD5XzbyKjpQZuirUI4SLFtqFb7YLkeYHeFA1TanihCeDii2rkC+sQJPQOA1Y74Z+PfB7DhUsdnGW4cEUUvz4eMOmCk0Ntr8SdVJWXn8ubCTwanmJiMbdK5S9U9R7tzs/IflUh3IzHzPEssF4d+rJSMD+VfmGsxJWn+Vmd4bVIjTCPwdW6BjqdARnRZh8slsRWhFD3O4vXvxPn12mQz6QBlxJBwpNcZk6npqJkX7UQx765lIK6hHqlXLauCvdNgWhW6QFCnEqsuMT/xOU0GL2JXugTDz6ghhJwslKSrLPFDtiE4FN+OvEUXa1rEe7H536QiuWLxbXyqyZH5mCQLDJ+tVkjnvp4WHhRXWX33Wi3Fg74yhYdOq7IUMeaL8xmxA7l312HN9Y=\"\n" +
                "\n" +
                "# Download the JAR file from the S3 bucket\n" +
                "aws s3 cp s3://myjarsbucket/Worker.jar /tmp/Worker.jar\n" +
                "\n" +
                "# Run the JAR file\n" +
                "java -jar /tmp/Worker.jar\n";




       // String workerQueueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
       // int numberOfFiles = aws.getQueueSize(workerQueueUrl);
        //int numberOfActiveWorkers = aws.getInstancesByTag(AWS.Node.WORKER.name()).size();
       // int jobsPerWorker = n;
       // int numOfNewInstances = (int) (Math.ceil((numberOfFiles / jobsPerWorker)) - numberOfActiveWorkers);
       //if (numOfNewInstances > 4) {
        //    System.out.println("9+ new instances requested. WE DON'T WANNA GET BANNED.");
        //    return;
        //}
        //if (numOfNewInstances > 0)
            //aws.createEC2(script2, AWS.Node.WORKER.name(), 1);
        //else {
        //    int terminateNumber = Math.abs(numOfNewInstances);
        //    List<Instance> workers = aws.getInstancesByTag(AWS.Node.WORKER.name());
        //    for (int i = 0; i < terminateNumber; i++) {
        //        String workerId = workers.get(i).instanceId();
        //        aws.terminateInstance(workerId);
        //    }
        //}
    }

    // 4a. (Taken from System Summary) Manager reads all Workers' messages from SQS
    public static List<String> getAllWorkersMessages(AWS aws) {
        List<String> results = new ArrayList<>();
        String workerResultsQueueUrl = aws.getQueueUrl(AWS.WORKER_TO_MANAGER_QUEUE_NAME);

        System.out.println("Waiting for worker results...");
        for (int i = 0; i < 6; i++) { //#TODO change it to a while loop that will pass everything later (did this to check it works)
            List<Message> workerMessages = aws.receiveMessagesFromSQS(workerResultsQueueUrl);
            if (workerMessages.isEmpty()) {
                break;
            }

            for (Message workerMessage : workerMessages) {
                String workerMessageBody = workerMessage.body();
                System.out.println("Received worker result: " + workerMessageBody);
                results.add(workerMessageBody);
                //aws.deleteMessageFromSQS(AWS.WORKER_TO_MANAGER_QUEUE_NAME, workerMessage.receiptHandle());
                //System.out.println("Deleted processed worker message from queue.");
            }
        }
        return results;
    }

    // 4b. creates one summary file, once all URLs in the input file have been
    // processed.
    private static File createSummaryFile(List<String> results) {
        File summaryFile = new File("summary.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFile))) {
            for (String result : results) {
                writer.write(result);
                writer.newLine();
            }
            System.out.println("Summary file created: " + summaryFile.getPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return summaryFile;
    }

    // 5. (Taken from System Summary) Manager uploads the summary file to S3.
    // 6. (Taken from System Summary) Manager posts an SQS message about the summary
    // file
    private static void uploadFileToS3AndSQS(AWS aws, File summaryFile) {
        String s3SummaryKey = "summary/" + summaryFile.getName();
        String path = "";
        try {
            path = aws.uploadFileToS3(s3SummaryKey, summaryFile);
            System.out.println("Uploaded summary file to S3: " + path);  // Corrected URL format
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to upload summary file to S3: " + e.getMessage());
        }
    
        try {
            aws.sendMessageToSQS(AWS.LOCAL_MANAGER_QUEUE_NAME, path);  // Corrected URL format
            System.out.println(
                    "Message sent to LOCAL_MANAGER_QUEUE_NAME with summary file information: " + path);  // Corrected URL format
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send summary file message to SQS: " + e.getMessage());
        }
    }
    
}
