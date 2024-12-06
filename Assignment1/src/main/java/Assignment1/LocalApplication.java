package Assignment1;

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

    public static void main(String[] args) { // args = [inputFileName, outputFileName, n, terminateSymbole]

        AWS.debug("LocalApplication Application is up");

        // creat AWS helper instance
        AWS aws = AWS.getAWSInstance();
        AWS.debug("aws was created");


        // Parse input
         if (args.length != 4) {
             AWS.debug("Error: Expected 4 arguments - [inputFileName, outputFileName, n, terminateSymbol]");
             System.exit(1);
         }
        String inputFileName = args[0];
        String outputFileName = args[1];
        String filesPerWorkers = args[2];
        String terminateSymbol = args[3];

        // 1.
        createManager(aws);
        // 2 + 3
        uploadInputToS3(aws, inputFileName);
        // 4 + 5
        //handleSummaryFile(aws);

        // #TODO 6

    }

    // 1. Checks if a Manager node is active on the EC2 cloud. If it is not, the
    // application will start the manager node.
    private static void createManager(AWS aws) {
        List<Instance> managerInstance = aws.getInstancesByTag(AWS.Node.MANAGER.name());
        if (managerInstance.isEmpty()) {
            AWS.debug("Manager instance not found. Starting a new one...");
            String managerScript = "todo"; // TODO: write a manager script
            aws.createEC2(managerScript, AWS.Node.MANAGER.name(), 1);
        } else {
            AWS.debug("Manager already exists.");
        }
    }

    // 2. Uploads the file to S3
    // 3. Sends a message to an SQS queue, stating the location of the file on S3
    private static void uploadInputToS3(AWS aws, String inputFileName) {
        aws.createBucketIfDoesntExists();
    
        File inputFile = new File(inputFileName); // inputfile should be in the same folder
        String keyPath = "inputFiles/" + inputFile.getName();
    
        if (aws.doesFileExistInS3(keyPath)) {
            AWS.debug("File " + keyPath + " already exists in S3. Skipping upload.");
            return; 
        }
    
        String s3Url = aws.uploadFileToS3(keyPath, inputFile);
    
        if (s3Url != null) {
            aws.sendMessageToSQS(AWS.LOCAL_MANAGER_QUEUE_NAME, s3Url); 
        } else {
            AWS.debug("File upload failed, message not sent to SQS.");
        }
    }
    

    // 4. Checks an SQS queue for a message indicating the process is done and the
    // response (the summary file) is available on S3.
    // 5. Creates an html file representing the results
    private static void handleSummaryFile(AWS aws) {

        //////////////// Downlod the summery file //////////////////////
        String queueUrl = aws.getQueueUrl(AWS.LOCAL_MANAGER_QUEUE_NAME);
        List<Message> messages = aws.receiveMessagesFromSQS(queueUrl);

        if (messages.isEmpty()) {
            System.out.println("[INFO] No messages in the queue.");
            return;
        }

        Message message = messages.get(0); // get the output
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
