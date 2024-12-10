package Assignment1;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import javax.imageio.ImageIO;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.text.PDFTextStripper;

public class PDFHandler {

    public static void downloadFileFromUrl(String urlString, File destinationFile) throws IOException {
        URL url = new URL(urlString);
        Files.copy(url.openStream(), destinationFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    // Helper method to generate a unique file name
    private static String generateUniqueFileName(String originalFileName) {
        String uniqueId = UUID.randomUUID().toString();
        String fileExtension = originalFileName.substring(originalFileName.lastIndexOf("."));
        String fileNameWithoutExtension = originalFileName.substring(0, originalFileName.lastIndexOf("."));
        return fileNameWithoutExtension + "-" + uniqueId + fileExtension;
    }

    // Method to convert the first page of the PDF to a PNG image
    public static String convertPdfToPng(String pdfPath, String outputFilePath) {
        File pdfFile = new File(pdfPath);
        String uniqueOutputFilePath = null;
        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFRenderer renderer = new PDFRenderer(document);

            // Render only the first page (index 0)
            BufferedImage image = renderer.renderImageWithDPI(0, 300, ImageType.RGB);

            // Generate a unique output file name for the first page
            uniqueOutputFilePath = generateUniqueFileName(outputFilePath);
            File outputFile = new File(uniqueOutputFilePath); // Saving in the default folder

            // Attempt to write the image
            try {
                if (!ImageIO.write(image, "PNG", outputFile)) {
                    writeErrorToFile(new File(uniqueOutputFilePath), "Error writing PNG image.");
                }
            } catch (IOException e) {
                writeErrorToFile(new File(uniqueOutputFilePath), "Error converting page to PNG: " + e.getMessage());
            }
        } catch (IOException e) {
            writeErrorToFile(new File(uniqueOutputFilePath), "Error processing PDF file: " + e.getMessage());
        }
        return uniqueOutputFilePath;
    }

    // Method to convert PDF to a text file and save in the default directory
    public static String convertPdfToText(String pdfPath, String outputFilePath) {
        File pdfFile = new File(pdfPath);
        String uniqueOutputFilePath = null;
        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFTextStripper textStripper = new PDFTextStripper();
            String text = textStripper.getText(document);

            // Generate a unique output file name
            uniqueOutputFilePath = generateUniqueFileName(outputFilePath);

            try (FileWriter writer = new FileWriter(uniqueOutputFilePath)) { // Saving in the default folder
                writer.write(text);
            } catch (IOException e) {
                writeErrorToFile(new File(uniqueOutputFilePath), "Error writing text file: " + e.getMessage());
            }
        } catch (IOException e) {
            writeErrorToFile(new File(uniqueOutputFilePath), "Error converting PDF to text: " + e.getMessage());
        }
        return uniqueOutputFilePath;
    }

    // Method to convert PDF to a basic HTML file and save in the default directory
    public static String convertPdfToHtml(String pdfPath, String outputFilePath) {
        File pdfFile = new File(pdfPath);
        String uniqueOutputFilePath = null;
        try (PDDocument document = PDDocument.load(pdfFile)) {
            PDFTextStripper textStripper = new PDFTextStripper();
            String text = textStripper.getText(document);

            // Wrap text in basic HTML tags
            String htmlContent = "<html><body><pre>" + text + "</pre></body></html>";

            // Generate a unique output file name
            uniqueOutputFilePath = generateUniqueFileName(outputFilePath);

            try (FileWriter writer = new FileWriter(uniqueOutputFilePath)) { // Saving in the default folder
                writer.write(htmlContent);
            } catch (IOException e) {
                writeErrorToFile(new File(uniqueOutputFilePath), "Error writing HTML file: " + e.getMessage());
            }
        } catch (IOException e) {
            writeErrorToFile(new File(uniqueOutputFilePath), "Error converting PDF to HTML: " + e.getMessage());
        }
        return uniqueOutputFilePath;
    }

    // Helper method to write error messages to the same file name in the default
    // directory
    private static void writeErrorToFile(File file, String errorMessage) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(file, true))) {
            writer.println("Error: " + errorMessage);
        } catch (IOException e) {
            // Suppress any errors during error logging
        }
    }

}
