package Assignment1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
    String filesPerWorkers = args[2];
    boolean terminate = args.length > 3 && args[3] != null;

    // Threads for different tasks
    Thread inputFileHandlerThread = new Thread(() -> handleInputFiles(aws, filesPerWorkers, terminate));
    Thread summaryHandlerThread = new Thread(() -> handleSummaryFiles(aws, terminate));
    Thread workersHandlerThread = new Thread(() -> handleWorkersPeriodically(aws, filesPerWorkers, terminate));

    // Start threads
    inputFileHandlerThread.start();
    summaryHandlerThread.start();
    workersHandlerThread.start();
}

private static void handleInputFiles(AWS aws, String filesPerWorkers, boolean terminate) {
    try {
        while (true) {
            Message message = receiveSQSMessage(aws);
            if (message != null && message.body().contains("inputFiles") && !terminate) {
                try {
                    File inputFile = downloadInputFromS3(aws, message.body());
                    processInputFileToSQS(aws, inputFile);
                    AWS.debug("Removing message:");
                    removeMessageFromSQS(aws, message);
                } catch (Exception e) {
                    System.err.println("[ERROR] An error occurred in inputFileHandler: " + e.getMessage());
                }
            } else if (terminate) {
                handleTerminate(aws);
                Thread.sleep(1000 * 5); // Sleep for 5 seconds
            } else {
                Thread.sleep(1000); // Sleep for 1 second
            }
        }
    } catch (Exception e) {
        System.err.println("[ERROR] Exception in inputFileHandler thread: " + e.getMessage());
    }
}

private static void handleSummaryFiles(AWS aws, boolean terminate) {
    try {
        while (true) {
            Message message = receiveSQSMessage(aws);
            if (message != null && message.body().contains("summary")) {
                try {
                    List<String> results = getAllWorkersMessages(aws);
                    File summaryFile = createSummaryFile(results);
                    uploadFileToS3AndSQS(aws, summaryFile);
                    AWS.debug("Removing message:");
                    removeMessageFromSQS(aws, message);
                } catch (Exception e) {
                    System.err.println("[ERROR] An error occurred in summaryHandler: " + e.getMessage());
                }
            } else if (terminate) {
                handleTerminate(aws);
                Thread.sleep(1000 * 5); // Sleep for 5 seconds
            } else {
                Thread.sleep(1000); // Sleep for 1 second
            }
        }
    } catch (Exception e) {
        System.err.println("[ERROR] Exception in summaryHandler thread: " + e.getMessage());
    }
}

private static void handleWorkersPeriodically(AWS aws, String filesPerWorkers, boolean terminate) {
    try {
        while (true) {
            // Call the handleWorkers method every 10 seconds
            handleWorkers(aws, Integer.parseInt(filesPerWorkers));
            Thread.sleep(1000 * 10); // Sleep for 10 seconds
        }
    } catch (Exception e) {
        System.err.println("[ERROR] Exception in workersHandler thread: " + e.getMessage());
    }
}


    // Gets the message of the location of the given input file.
    private static Message receiveSQSMessage(AWS aws) {
        String queueUrl = aws.getQueueUrl(AWS.LOCAL_MANAGER_QUEUE_NAME);
        List<Message> messages = aws.receiveMessagesFromSQS(queueUrl);

        if (messages.isEmpty()) {
            System.out.println("No messages in the queue.");
            return null;
        }

        Message message = messages.get(0);
        System.out.println("Received message from SQS: " + message.body());

        return message;
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
    private static void processInputFileToSQS(AWS aws, File inputFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            AWS.debug("Starting to processInputFile...");
            AWS.debug_messages = false;
            while ((line = reader.readLine()) != null) {

                if (!line.trim().isEmpty() && line.contains("\t")) {
                    aws.sendMessageToSQS(AWS.MANAGER_TO_WORKER_QUEUE_NAME, line.trim());
                    // AWS.debug("Message sent to worker queue: " + line);
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

    // Delete SQS message used
    private static void removeMessageFromSQS(AWS aws, Message message) {
        try {
            String receiptHandle = message.receiptHandle();
            aws.deleteMessageFromSQS(AWS.LOCAL_MANAGER_QUEUE_NAME, receiptHandle);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to delete message from SQS: " + e.getMessage());
        }
    }

    // 3. Checks the SQS message count and starts Worker processes (nodes)
    // accordingly.
    private static synchronized void handleWorkers(AWS aws, int n) {
        String workerScript = "#!/bin/bash\n"
                + "echo Worker jar running\n"
                + "echo s3://myjarsbucket/Worker.jar\n"
                + "mkdir WorkerFiles\n"
                + "aws s3 cp s3://myjarsbucket/Worker.jar ./WorkerFiles/Worker.jar\n"
                + "echo worker copied the jar from s3\n"
                + "java -jar ./WorkerFiles/Worker.jar\n";

        String workerQueueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
        int numberOfFiles = aws.getQueueSize(workerQueueUrl);
        int numberOfActiveWorkers = aws.getRunningInstancesByTag(AWS.Node.WORKER.name()).size();
        int jobsPerWorker = n;
        int numOfNewInstances = (int) (Math.ceil((numberOfFiles / jobsPerWorker)) - numberOfActiveWorkers);
        if (numOfNewInstances > 7) {
            System.out.println("Wanted " + numOfNewInstances + ". 7+ new instances requested?? WE DON'T WANNA GET BANNED.");
            return;
        }
        if (numOfNewInstances > 0) {
            //aws.createEC2(workerScript, AWS.Node.WORKER.name(), numOfNewInstances);
        } else if (numberOfFiles > 0 && numberOfActiveWorkers == 0) {
            //aws.createEC2(workerScript, AWS.Node.WORKER.name(), 1);
        }
    }

    public static long countLines(File file) throws IOException {
        return Files.lines(file.toPath()).count();
    }

    // 4a. (Taken from System Summary) Manager reads all Workers' messages from SQS
    public static List<String> getAllWorkersMessages(AWS aws) {
        List<String> results = new ArrayList<>();
        String workerResultsQueueUrl = aws.getQueueUrl(AWS.WORKER_TO_MANAGER_QUEUE_NAME);

        System.out.println("Waiting for worker results...");
        for (int i = 0; i < 6; i++) { // #TODO change it to a while loop that will pass everything later (did this to
            // check it works)
            List<Message> workerMessages = aws.receiveMessagesFromSQS(workerResultsQueueUrl);
            if (workerMessages.isEmpty()) {
                break;
            }

            for (Message workerMessage : workerMessages) {
                String workerMessageBody = workerMessage.body();
                System.out.println("Received worker result: " + workerMessageBody);
                results.add(workerMessageBody);
                // aws.deleteMessageFromSQS(AWS.WORKER_TO_MANAGER_QUEUE_NAME,
                // workerMessage.receiptHandle());
                // System.out.println("Deleted processed worker message from queue.");
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
        // Generate a unique identifier for the file key
        String uniqueId = System.currentTimeMillis() + "-" + UUID.randomUUID();
        String s3SummaryKey = "summary/" + uniqueId + "-" + summaryFile.getName();
        AWS.debug("Generated unique key path for S3: " + s3SummaryKey);
        String path = "";
        try {
            // Upload the file to S3 with the unique key
            path = aws.uploadFileToS3(s3SummaryKey, summaryFile);
            System.out.println("Uploaded summary file to S3: " + path); // Corrected URL format
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to upload summary file to S3: " + e.getMessage());
            return; // Exit early if upload fails
        }

        try {
            // Send the S3 file URL to the SQS queue
            aws.sendMessageToSQS(AWS.LOCAL_MANAGER_QUEUE_NAME, path);
            System.out.println(
                    "Message sent to LOCAL_MANAGER_QUEUE_NAME with summary file information: " + path);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send summary file message to SQS: " + e.getMessage());
        }
    }

    //7
    private static void handleTerminate(AWS aws) {
        if (aws.getInstancesByTag(AWS.Node.WORKER.name()).isEmpty()) {
            aws.terminateInstance(aws.getInstanceId());
        }
    }

}
