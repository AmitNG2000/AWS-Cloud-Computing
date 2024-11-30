import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

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
  
    // Method to convert PDF to PNG images
    public static void convertPdfToPng(String pdfPath, String outputDir) throws IOException {
        try (PDDocument document = PDDocument.load(new File(pdfPath))) {
            PDFRenderer renderer = new PDFRenderer(document);
            for (int page = 0; page < document.getNumberOfPages(); page++) {
                BufferedImage image = renderer.renderImageWithDPI(page, 300, ImageType.RGB);
                File outputFile = new File(outputDir, "page-" + (page + 1) + ".png");
                ImageIO.write(image, "PNG", outputFile);
                System.out.println("Saved PNG: " + outputFile.getAbsolutePath());
            }
        }
    }

    // Method to convert PDF to a text file
    public static void convertPdfToText(String pdfPath, String textFilePath) throws IOException {
        try (PDDocument document = PDDocument.load(new File(pdfPath))) {
            PDFTextStripper textStripper = new PDFTextStripper();
            String text = textStripper.getText(document);

            try (FileWriter writer = new FileWriter(textFilePath)) {
                writer.write(text);
                System.out.println("Saved text file: " + textFilePath);
            }
        }
    }

    // Method to convert PDF to a basic HTML file
    public static void convertPdfToHtml(String pdfPath, String htmlFilePath) throws IOException {
        try (PDDocument document = PDDocument.load(new File(pdfPath))) {
            PDFTextStripper textStripper = new PDFTextStripper();
            String text = textStripper.getText(document);

            // Wrap text in basic HTML tags
            String htmlContent = "<html><body><pre>" + text + "</pre></body></html>";

            try (FileWriter writer = new FileWriter(htmlFilePath)) {
                writer.write(htmlContent);
                System.out.println("Saved HTML file: " + htmlFilePath);
            }
        }
    }
}
