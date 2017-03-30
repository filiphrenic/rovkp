package hr.fer.tel.rovkp.dz1.zad2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Read files from one directory and copy all lines to a separate file
ite */
public class Gutenberg {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Expecting 2 arguments: <path-to-folder> <output-file>");
			return;
		}

		Path directory = Paths.get(args[0]);
		Path outputFilename = Paths.get(args[1]);

		if (!Files.isDirectory(directory)) {
			System.err.println("First argument isn't a directory");
			return;
		}

		if (Files.exists(outputFilename)) {
			System.err.println("File " + outputFilename + " already exists");
			return;
		} else {
			Files.createFile(outputFilename);
		}

		Copier copier = new Copier(Files.newBufferedWriter(outputFilename));
		Files.walkFileTree(directory, copier);
		System.out.println("Copied " + copier.numLines + " lines");
	}

	private static class Copier extends SimpleFileVisitor<Path> {
		private BufferedWriter writer;
		private long numLines;

		Copier(BufferedWriter writer) {
			this.writer = writer;
			numLines = 0;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

			BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.ISO_8859_1);

			while (true) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				writer.write(line);
				numLines++;
			}
			writer.flush();

			return FileVisitResult.CONTINUE;
		}
	}
}
