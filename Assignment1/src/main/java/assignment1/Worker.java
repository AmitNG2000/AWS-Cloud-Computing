package assignment1;

import software.amazon.awssdk.services.sqs.model.Message;
import java.io.File;
import java.io.IOException;

/*
 * The Workers
 * 1. Gets a message from SQS Queue.
 * 2. Download the PDF file indicate in the message.
 * 3. Perform the operation requested on the file.
 * 4. Upload the resulting output file to S3
 * 5. Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new image file, and the operation that was performed.
 * 6. remove the processed message from the SQS queue.
 */

public class Worker {
    public static void main(String[] args) {
        AWS aws = AWS.getAWSInstance();
        String s3KeyPath = "";

        //1.
        String messageBody = getPDFMessageFromSQS(aws);
        //2.
        String[] parts = messageBody.split("\t");
        if (parts.length != 2) {
            System.err.println("[ERROR] Invalid message format: " + messageBody);
            return;
        }
        String operation = parts[0];
        String pdfUrl = parts[1];
        File downloadedFile = downloadPDF(messageBody);
        //3.
        String outputFilePath = processPDF(operation, downloadedFile);
        //4.
        s3KeyPath = uploadResultToS3(aws, outputFilePath, s3KeyPath);
        //5.
        uploadResultToSQS(aws, s3KeyPath, operation, pdfUrl);
        //6.
        removePDFMessageFromSQS(aws);      
    }

    //1. Gets a message from SQS Queue.
    private static String getPDFMessageFromSQS(AWS aws) {
        String queueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
        if (queueUrl == null) {
            System.err.println("[ERROR] Queue does not exist.");
            return "";
        }
        System.out.println("[DEBUG] Receiving messages...");
        Message message = aws.receiveMessagesFromSQS(queueUrl).stream().findFirst().orElse(null);
        if (message == null) {
            System.out.println("[INFO] No messages in the queue.");
            return "";
        }

        return message.body();
    }

    //2. Download the PDF file indicate in the message.
    private static File downloadPDF(String messageBody) {
        String[] parts = messageBody.split("\t");
        if (parts.length != 2) {
            System.err.println("[ERROR] Invalid message format: " + messageBody);
            return null;
        }
        String pdfUrl = parts[1];
        File downloadedFile = new File("downloaded.pdf");
        System.out.println("[DEBUG] Downloading PDF from URL: " + pdfUrl);
        try {
            PDFHandler.downloadFileFromUrl(pdfUrl, downloadedFile);
            System.out.println("[INFO] PDF file downloaded successfully: " + downloadedFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to download PDF: " + e.getMessage());
            return null;
        }
        return downloadedFile;
    }

    //3. Perform the operation requested on the file.
    private static String processPDF(String operation, File downloadedFile) {
        String outputFilePath = null;
        try {
            switch (operation) {
                case "ToImage":
                    outputFilePath = "output.png"; 
                    PDFHandler.convertPdfToPng(downloadedFile.getAbsolutePath(), outputFilePath);
                    break;
                case "ToText":
                    outputFilePath = "output.txt"; 
                    PDFHandler.convertPdfToText(downloadedFile.getAbsolutePath(), outputFilePath);
                    break;
                case "ToHTML":
                    outputFilePath = "output.html"; 
                    PDFHandler.convertPdfToHtml(downloadedFile.getAbsolutePath(), outputFilePath);
                    break;
                default:
                    System.err.println("[ERROR] Unknown operation: " + operation);
                    return "";
            }
            System.out.println("[INFO] Processing complete for operation: " + operation);
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to process PDF: " + e.getMessage());
            return "";
        }

        return outputFilePath;
    }

    //4. Upload the resulting output file to S3
    private static String uploadResultToS3(AWS aws, String outputFilePath, String s3KeyPath) {
        try {
            System.out.println("[DEBUG] Uploading result to S3...");
            File resultFile = new File(outputFilePath);
            s3KeyPath = "results/" + resultFile.getName(); 
            aws.uploadFileToS3(s3KeyPath, resultFile);
            System.out.println("[INFO] Uploaded to S3: " + s3KeyPath);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to upload result to S3: " + e.getMessage());
        }

        return s3KeyPath;
    }
    
    //5. Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new image file, and the operation that was performed.
    private static void uploadResultToSQS(AWS aws, String s3KeyPath, String operation, String pdfUrl){
        String messageBodyToSend = s3KeyPath + "\t" + operation + "\t" + pdfUrl;

        try {
            aws.sendMessageToSQS(AWS.WORKER_TO_MANAGER_QUEUE_NAME, messageBodyToSend);
            System.out.println("[INFO] Message sent to SQS.");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send message to SQS: " + e.getMessage());
        }
    }

    //6. remove the processed message from the SQS queue.
    private static void removePDFMessageFromSQS(AWS aws) {
        String queueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
        Message message = aws.receiveMessagesFromSQS(queueUrl).stream().findFirst().orElse(null);

        try {
            String receiptHandle = message.receiptHandle();
            aws.deleteMessageFromSQS(AWS.MANAGER_TO_WORKER_QUEUE_NAME, receiptHandle);
            System.out.println("[INFO] Message deleted from SQS queue.");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to delete message from SQS: " + e.getMessage());
        }
    }
}