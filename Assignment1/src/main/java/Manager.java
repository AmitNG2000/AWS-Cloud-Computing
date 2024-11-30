import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.ec2.model.*;

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
        //1.
        String s3FileKey = receiveInputMessage(aws);
        File outputFile = downloadInputFromS3(aws, s3FileKey);
        //2.
        processInputFileToSQS(aws, outputFile);
        
        // Start a thread to handle worker messages continuously
        Thread workerHandlerThread = new Thread(() -> {
            try {
                while (true) {
                    //3.
                    handleWorkers(aws, Integer.parseInt(args[2]));
                    List<String> results = getAllWorkersMessages(aws);
                    //4.
                    File summaryFile = createSummaryFile(results);
                    //5. + 6.
                    uploadFileToS3AndSQS(aws, summaryFile);

                    //#TODO 7.

                    Thread.sleep(1000); // Sleep for 1 seconds
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Exception in worker handling thread: " + e.getMessage());
                e.printStackTrace();
            }
        });

        workerHandlerThread.start(); // Start the thread
    }

    //Gets the message of the location of the given input file. 
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

    //1. Downloads the input file from S3.
    private static File downloadInputFromS3(AWS aws, String s3FileKey) {
        File outputFile = new File("localFiles/" + new File(s3FileKey).getName());
        aws.downloadFileFromS3(s3FileKey, outputFile);                                                  
        System.out.println("Input file downloaded and ready for processing: " + outputFile.getPath());
        return outputFile;
    }

    //2. Creates an SQS message for each URL in the input file together with the operation that should be performed on it.
    private static void processInputFileToSQS(AWS aws, File outputFile) {
        String workerQueueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
        try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().equals("terminate")) { // #TODO Ask how terminate message is given
                    // #TODO TERMINATE EVERYTHING THEY WANT
                    break;
                }

                if (!line.trim().isEmpty() && line.contains("\t")) {
                    aws.sendMessageToSQS(workerQueueUrl, line.trim());
                    System.out.println("Message sent to worker queue: " + line);
                } else {
                    System.err.println("Skipping invalid line: " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //3. Checks the SQS message count and starts Worker processes (nodes) accordingly.
    private static void handleWorkers(AWS aws, int n) {
         String workerQueueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
         int numberOfFiles = aws.getQueueSize(workerQueueUrl);
         int numberOfActiveWorkers = aws.getInstancesByTag(AWS.Node.WORKER.name()).size();
         int jobsPerWorker = n;
         int numOfNewInstances = (int) (Math.ceil((numberOfFiles / jobsPerWorker)) - numberOfActiveWorkers);
         if (numOfNewInstances > 12) {
             System.out.println("12+ new instances requested. WE DON'T WANNA GET BANNED.");
             return;
         }
         if (numOfNewInstances > 0)
             aws.createEC2("SCRIPTHERE", AWS.Node.WORKER.name(), numOfNewInstances);
         else {
             int terminateNumber = Math.abs(numOfNewInstances);
             List<Instance> workers = aws.getInstancesByTag(AWS.Node.WORKER.name());
             for (int i = 0; i < terminateNumber; i++) {
                 String workerId = workers.get(i).instanceId();
                 aws.terminateInstance(workerId);
             }
         }
    }

    //4a. (Taken from System Summary) Manager reads all Workers' messages from SQS
    public static List<String> getAllWorkersMessages(AWS aws) {
        List<String> results = new ArrayList<>();
        String workerResultsQueueUrl = aws.getQueueUrl(AWS.WORKER_TO_MANAGER_QUEUE_NAME);

        System.out.println("Waiting for worker results...");
        while (true) {
            List<Message> workerMessages = aws.receiveMessagesFromSQS(workerResultsQueueUrl);
            if (workerMessages.isEmpty()) {
                break;
            }

            for (Message workerMessage : workerMessages) {
                String workerMessageBody = workerMessage.body();
                System.out.println("Received worker result: " + workerMessageBody);
                results.add(workerMessageBody);
                aws.deleteMessageFromSQS(AWS.WORKER_TO_MANAGER_QUEUE_NAME, workerMessage.receiptHandle());
                System.out.println("Deleted processed worker message from queue.");
            }

            try {
                Thread.sleep(1000); // 1 second delay
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return results;
    }
    //4b. creates one summary file, once all URLs in the input file have been processed.
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

    //5. (Taken from System Summary) Manager uploads the summary file to S3.
    //6. (Taken from System Summary) Manager posts an SQS message about the summary file
    private static void uploadFileToS3AndSQS(AWS aws, File summaryFile) {
        try {
            String s3SummaryKey = "summary/" + summaryFile.getName();
            aws.uploadFileToS3(s3SummaryKey, summaryFile);
            System.out.println("Uploaded summary file to S3: s3://your-bucket-name/" + s3SummaryKey);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to upload summary file to S3: " + e.getMessage());
        }

        String summaryFilePath = summaryFile.getAbsolutePath();
        String summaryMessage = "Summary file created: " + summaryFilePath;

        try {
            aws.sendMessageToSQS(AWS.LOCAL_MANAGER_QUEUE_NAME, summaryMessage);
            System.out.println(
                    "Message sent to LOCAL_MANAGER_QUEUE_NAME with summary file information: " + summaryMessage);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send summary file message to SQS: " + e.getMessage());
        }
    }
}
