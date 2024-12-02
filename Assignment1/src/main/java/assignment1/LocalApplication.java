package assignment1;

import java.io.File;
import java.util.List;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.ec2.model.*;

/*
 * Local Application
 * 1. Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the manager node. 
 * 2. Uploads the file to S3
 * 3. Sends a message to an SQS queue, stating the location of the file on S3
 * 4. Checks an SQS queue for a message indicating the process is done and the response (the summary file) is available on S3.
 * 5. Creates an html file representing the results
 * 6. In case of terminate mode (as defined by the command-line argument), sends a termination message to the Manager
 */

public class LocalApplication {

    public static void main(String[] args) {

        System.out.println("[MY_DEbug] LocalApplication Application is up");
        AWS aws = AWS.getAWSInstance();
        // 1.
        createManager(aws);
        // 2 + 3
        handleInput(aws, args[0]);
        // 4 + 5
        handleSummaryFile(aws);

        // #TODO 6

    }

    // 1. Checks if a Manager node is active on the EC2 cloud. If it is not, the
    // application will start the manager node.
    private static void createManager(AWS aws) {
        List<Instance> managerInstance = aws.getInstancesByTag(AWS.Node.MANAGER.name());
        if (managerInstance.isEmpty()) {
            System.out.println("Manager instance not found. Starting a new one...");
            aws.runInstanceFromAMI(AWS.IMAGE_AMI); // #TODO add my own IMAGE AMI
        }
    }

    // 2. Uploads the file to S3
    // 3. Sends a message to an SQS queue, stating the location of the file on S3
    private static void handleInput(AWS aws, String inputFileName) {
        File inputFile = new File(inputFileName);

        String keyPath = "inputFiles/" + inputFile.getName();
        String s3Url = aws.uploadFileToS3(keyPath, inputFile);

        if (s3Url != null) {
            aws.sendMessageToSQS(AWS.LOCAL_MANAGER_QUEUE_NAME, s3Url);
        } else {
            System.err.println("File upload failed, message not sent to SQS.");
        }
    }

    // 4. Checks an SQS queue for a message indicating the process is done and the
    // response (the summary file) is available on S3.
    // 5. Creates an html file representing the results
    private static void handleSummaryFile(AWS aws) {
        String queueUrl = aws.getQueueUrl(AWS.LOCAL_MANAGER_QUEUE_NAME);
        List<Message> messages = aws.receiveMessagesFromSQS(queueUrl);

        if (messages.isEmpty()) {
            System.out.println("[INFO] No messages in the queue.");
            return;
        }

        Message message = messages.get(0);
        String summaryMessage = message.body();
        System.out.println("[INFO] Received summary message from SQS: " + summaryMessage);

        String localSummaryFilePath = "localFiles/summary_file.txt";
        File summaryFile = new File(localSummaryFilePath);

        try {
            aws.downloadFileFromS3(summaryMessage, summaryFile);
            System.out.println("[INFO] Summary file downloaded successfully from S3: " + summaryFile.getAbsolutePath());
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to download summary file from S3: " + e.getMessage());
        }
    }
}
