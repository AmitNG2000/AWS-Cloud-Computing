package Assignment1;

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
        File downloadedFile = null;
        String outputFilePath = "";
    
        try {
            // 1.
            String messageBody = getPDFMessageFromSQS(aws);
    
            // 2.
            String[] parts = messageBody.split("\t");
            if (parts.length != 2) {
                System.err.println("[ERROR] Invalid message format: " + messageBody);
                return;
            }
            String operation = parts[0];
            String pdfUrl = parts[1];
            downloadedFile = downloadPDF(messageBody);
            if (downloadedFile == null) {
                AWS.debug("PDF that was downloaded had an error. Pass.");
                removePDFMessageFromSQS(aws);
                return;
            }
    
            // 3.
            outputFilePath = processPDF(operation, downloadedFile);
            if (outputFilePath.isEmpty()) {
                AWS.debug("Processing the downloaded PDF resulted in an error. Pass.");
                removePDFMessageFromSQS(aws);
                return;
            }
    
            // 4.
            s3KeyPath = uploadResultToS3(aws, outputFilePath, s3KeyPath);
    
            // 5.
            uploadResultToSQS(aws, s3KeyPath, operation, pdfUrl);
    
            // 6.
            removePDFMessageFromSQS(aws);
        } finally {
            // Clean up resources
            if (downloadedFile != null && downloadedFile.exists()) {
                if (downloadedFile.delete()) {
                } else {
                    AWS.debug("Failed to delete downloaded file: " + downloadedFile.getAbsolutePath());
                }
            }
    
            if (outputFilePath != null && !outputFilePath.isEmpty()) {
                File outputFile = new File(outputFilePath);
                if (outputFile.exists()) {
                    if (outputFile.delete()) {
                    } else {
                        AWS.debug("Failed to delete output file: " + outputFilePath);
                    }
                }
            }
        }
    }
    

    // 1. Gets a message from SQS Queue.
    private static String getPDFMessageFromSQS(AWS aws) {
        String queueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
        if (queueUrl == null) {
            return "";
        }
        Message message = aws.receiveMessagesFromSQS(queueUrl).stream().findFirst().orElse(null);
        if (message == null) {
            AWS.debug("No messages in the queue: " + AWS.MANAGER_TO_WORKER_QUEUE_NAME);
            return "";
        }

        return message.body();
    }

    // 2. Download the PDF file indicate in the message.
    private static File downloadPDF(String messageBody) {
        String[] parts = messageBody.split("\t");
        if (parts.length != 2) {
            AWS.debug("Invalid message format: " + messageBody);
            return null;
        }
        String pdfUrl = parts[1];
        File downloadedFile = new File("downloaded.pdf");
        //AWS.debug("Downloading PDF from URL: " + pdfUrl);
        try {
            PDFHandler.downloadFileFromUrl(pdfUrl, downloadedFile);
            //AWS.debug("PDF file downloaded successfully: " + downloadedFile.getAbsolutePath());
        } catch (IOException e) {
            //AWS.debug("Failed to download PDF: " + e.getMessage());
            return null;
        }

        return downloadedFile;
    }

    // 3. Perform the operation requested on the file.
    private static String processPDF(String operation, File downloadedFile) {
        if (downloadedFile == null)
            return "";
        String outputFilePath = null;
        try {
            //AWS.debug("Received operation: " + operation.trim());
            switch (operation) {
                case "ToImage":
                    outputFilePath = "output.png";
                    outputFilePath = PDFHandler.convertPdfToPng(downloadedFile.getAbsolutePath(), outputFilePath);
                    break;
                case "ToText":
                    outputFilePath = "output.txt";
                    outputFilePath = PDFHandler.convertPdfToText(downloadedFile.getAbsolutePath(), outputFilePath);
                    break;
                case "ToHTML":
                    outputFilePath = "output.html";
                    outputFilePath = PDFHandler.convertPdfToHtml(downloadedFile.getAbsolutePath(), outputFilePath);
                    break;
                default:
                    //AWS.debug("[ERROR] Unknown operation: " + operation);
                    return "";
            }
            //AWS.debug("Processing complete for operation: " + operation);
        } catch (Exception e) {
            //AWS.debug("Failed to process PDF: " + e.getMessage());
            return "";
        }

        return outputFilePath;
    }

    // 4. Upload the resulting output file to S3
    private static String uploadResultToS3(AWS aws, String outputFilePath, String s3KeyPath) {
        try {
            System.out.println("[DEBUG] Uploading result to S3...");
            File resultFile = new File(outputFilePath);
            s3KeyPath = "results/" + resultFile.getName();
            aws.uploadFileToS3(s3KeyPath, resultFile);
            //System.out.println("[INFO] Uploaded to S3: " + s3KeyPath);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to upload result to S3: " + e.getMessage());
        }

        return s3KeyPath;
    }

    // 5. Put a message in an SQS queue indicating the original URL of the PDF, the
    // S3 url of the new image file, and the operation that was performed.
    private static void uploadResultToSQS(AWS aws, String s3KeyPath, String operation, String pdfUrl) {
        String messageBodyToSend = s3KeyPath.trim() + "\t" + operation.trim() + "\t" + pdfUrl.trim();

        try {
            aws.sendMessageToSQS(AWS.WORKER_TO_MANAGER_QUEUE_NAME, messageBodyToSend);
            //System.out.println("[INFO] Message sent to SQS.");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send message to SQS: " + e.getMessage());
        }
    }

    // 6. remove the processed message from the SQS queue.
    private static void removePDFMessageFromSQS(AWS aws) {
        String queueUrl = aws.getQueueUrl(AWS.MANAGER_TO_WORKER_QUEUE_NAME);
        Message message = aws.receiveMessagesFromSQS(queueUrl).stream().findFirst().orElse(null);

        try {
            String receiptHandle = message.receiptHandle();
            aws.deleteMessageFromSQS(AWS.MANAGER_TO_WORKER_QUEUE_NAME, receiptHandle);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to delete message from SQS: " + e.getMessage());
        }
    }
}