package Assignment1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;

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
        AWS.debug("LocalApplication Application is up");

        AWS aws = AWS.getAWSInstance();
        AWS.debug("AWS instance created.");

        String inputFileName = args[0];
        String outputFileName = args[1];
        String filesPerWorkers = args[2];
        String terminateSymbol = "";
        if (args.length > 3)
            terminateSymbol = args[3];

        createManager(aws, inputFileName, outputFileName, filesPerWorkers, terminateSymbol);
        uploadInputToS3(aws, inputFileName);

        // Handle summary file (waits for the "summary" message)
        handleSummaryFile(aws, outputFileName);
    }

    // 1 + 6. Checks if a Manager node is active on the EC2 cloud. If it is not, the
    // application will start the manager node.
    private static void createManager(AWS aws, String inputFileName, String outputFileName, String filesPerWorkers, String terminateSymbol) {
        String managerScript = "#!/bin/bash\n"
                + "echo Manager jar running\n"
                + "echo s3://myjarsbucket/Manager.jar\n"
                + "mkdir ManagerFiles\n"
                + "aws s3 cp s3://myjarsbucket/Manager.jar ./ManagerFiles/Manager.jar\n"
                + "echo manager copied the jar from s3\n"
                + "java -jar ./ManagerFiles/Manager.jar " + inputFileName + " " + outputFileName + " " + filesPerWorkers + " " + terminateSymbol + "\n";

        List<Instance> managerInstance = aws.getRunningInstancesByTag(AWS.Node.MANAGER.name());
        if (managerInstance.isEmpty()) {
            AWS.debug("Manager instance not found. Starting a new one...");
            aws.createEC2(managerScript, AWS.Node.MANAGER.name(), 1);
        } else {
            AWS.debug("Manager already exists.");
        }
    }

    // 2. Uploads the file to S3
    // 3. Sends a message to an SQS queue, stating the location of the file on S3
    private static void uploadInputToS3(AWS aws, String inputFileName) {
        aws.createBucketIfDoesntExists();

        File inputFile = new File(inputFileName); // inputFile should be in the same folder

        // Generate a unique identifier (timestamp + random UUID)
        String uniqueId = System.currentTimeMillis() + "-" + UUID.randomUUID();
        String keyPath = "inputFiles/" + uniqueId + "-" + inputFile.getName();
        String s3Url = aws.uploadFileToS3(keyPath, inputFile);

        if (s3Url != null) {
            AWS.debug("File successfully uploaded. S3 URL: " + s3Url);
            aws.sendMessageToSQS(AWS.LOCAL_MANAGER_QUEUE_NAME, s3Url);
        } else {
            AWS.debug("File upload failed, message not sent to SQS.");
        }
    }

    // 4. Checks an SQS queue for a message indicating the process is done and the
    // response (the summary file) is available on S3.
    // 5. Creates an html file representing the results
    private static void handleSummaryFile(AWS aws, String outputFileName) {
        String queueName = AWS.LOCAL_MANAGER_QUEUE_NAME;
        String summaryKeyword = "summary";

        // Wait until the "summary" message is received
        AWS.debug("Waiting for summary message..");
        String summaryMessage = waitForSQSMessage(aws, queueName, summaryKeyword);
        AWS.debug("Summary message found.");
        if (summaryMessage == null) {
            System.err.println("[ERROR] No summary message received.");
            return;
        }

        System.out.println("[INFO] Processing summary message: " + summaryMessage);

        File summaryFile = new File("summary_file.txt"); // Temporary file for the downloaded summary

        try {
            // Download the summary file from S3
            aws.downloadFileFromS3(summaryMessage, summaryFile); // Assumes summaryMessage contains the S3 key
            System.out.println("[INFO] Summary file downloaded successfully from S3: " + summaryFile.getAbsolutePath());

            // Create an HTML file from the summary file
            String localSummaryFilePath = outputFileName + ".html";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(localSummaryFilePath))) {
                writer.write("<html><head><title>Summary File</title></head><body><h1>Summary</h1>");
                try (BufferedReader reader = new BufferedReader(new FileReader(summaryFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write("<pre>" + line + "</pre>");
                    }
                }
                writer.write("</body></html>");
                System.out.println("[INFO] Summary HTML file created successfully: " + localSummaryFilePath);

            } catch (IOException e) {
                System.err.println("[ERROR] Failed to create summary HTML file: " + e.getMessage());
            }

        } catch (Exception e) {
            System.err.println("[ERROR] Failed to download summary file from S3: " + e.getMessage());
        }
    }

    private static String waitForSQSMessage(AWS aws, String queueName, String expectedKeyword) {
        String matchingMessageBody = null;

        while (matchingMessageBody == null) {
            List<Message> messages = aws.receiveMessagesFromSQS(queueName);

            for (Message message : messages) {
                if (message.body().contains(expectedKeyword)) {
                    matchingMessageBody = message.body();

                    // Delete the message using deleteMessageFromSQS
                    aws.deleteMessageFromSQS(queueName, message.receiptHandle());
                    break;
                }
            }

            if (matchingMessageBody == null) {
                try {
                    Thread.sleep(5000); // Avoid spamming the SQS queue
                } catch (InterruptedException e) {
                    System.err.println("[ERROR] Waiting for SQS message interrupted: " + e.getMessage());
                }
            }
        }

        return matchingMessageBody; // Return the body of the matching message
    }

}
