package Assignment1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

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
        handleSummaryFile(aws);

        // #TODO 6

    }

    // 1. Checks if a Manager node is active on the EC2 cloud. If it is not, the
    // application will start the manager node.
    private static void createManager(AWS aws) {
        String managerScript = "#!/bin/bash\n" +
        "echo Manager jar running\n" +
        "echo s3://myjarsbucket/Manager.jar\n" +
        "mkdir ManagerFiles\n" +
        "aws s3 cp s3://myjarsbucket/Manager.jar ./ManagerFiles/Manager.jar\n" +
        "echo manager copied the jar from s3\n" +
        "java -jar ./ManagerFiles/Manager.jar input-sample-1.txt outputFileName.txt 10 terminate\n"; //#TODO Enter the args values

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

        //////////////// Download the summary file //////////////////////
        String queueUrl = aws.getQueueUrl(AWS.LOCAL_MANAGER_QUEUE_NAME);
        List<Message> messages = aws.receiveMessagesFromSQS(queueUrl);
    
        if (messages.isEmpty()) {
            System.out.println("[INFO] No messages in the queue.");
            return;
        }

        /////////TERPORARY
        /* 
        try {
            String receiptHandle = messages.stream().findFirst().orElse(null).receiptHandle();
            aws.deleteMessageFromSQS(AWS.MANAGER_TO_WORKER_QUEUE_NAME, receiptHandle);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to delete message from SQS: " + e.getMessage());
        }
           */ 
    
        Message message = messages.stream().findFirst().orElse(null); // Get the output
        String summaryMessage = message.body();  // This contains the S3 key (filename)
        System.out.println("[INFO] Received summary message from SQS: " + summaryMessage);
    
        // Assuming the summary message contains the S3 filename to download
        File summaryFile = new File("summary_file.txt");  // Temporary file to store the downloaded summary
    
        try {
            // Download the summary file from S3 using the provided method
            aws.downloadFileFromS3(summaryMessage, summaryFile);  // Use the summaryMessage as the key
            System.out.println("[INFO] Summary file downloaded successfully from S3: " + summaryFile.getAbsolutePath());
    
            // After downloading, create the HTML file
            String localSummaryFilePath = "summary_file.html";  // HTML file path
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(localSummaryFilePath))) {
                // Writing the HTML content to the file
                writer.write("<html>");
                writer.write("<head><title>Summary File</title></head>");
                writer.write("<body>");
                writer.write("<h1>Summary</h1>");
                // Read content from the downloaded summary file and write it in the HTML file
                try (BufferedReader reader = new BufferedReader(new FileReader(summaryFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write("<pre>" + line + "</pre>");  // Using <pre> for preserving formatting
                    }
                } catch (IOException e) {
                    System.err.println("[ERROR] Failed to read downloaded summary file: " + e.getMessage());
                }
                writer.write("</body>");
                writer.write("</html>");
    
                System.out.println("[INFO] Summary HTML file created successfully: " + localSummaryFilePath);
            } catch (IOException e) {
                System.err.println("[ERROR] Failed to create summary HTML file: " + e.getMessage());
            }
    
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to download summary file from S3: " + e.getMessage());
        }
    }
    


}
