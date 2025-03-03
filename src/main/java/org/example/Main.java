package org.example;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;

public class Main {
	public static void main(String[] args) throws Exception {
		String url = "jdbc:mysql://localhost:4002/public";
		String user = "root";
		String password = "";

		// Load the MySQL JDBC driver.
		Class.forName("com.mysql.cj.jdbc.Driver");

		// Connect to GreptimeDB Edge.
		Connection connection = DriverManager.getConnection(url, user, password);

		Main main = new Main();

		// Create the news_articles table.
		//
		// +-------------+------------------------+-----+------+---------------------+---------------+
		// | Column | Type | Key | Null | Default | Semantic Type |
		// +-------------+------------------------+-----+------+---------------------+---------------+
		// | title | String | PRI | YES | | TAG |
		// | description | String | | YES | | FIELD |
		// | embedding | Vector(768) | | YES | | FIELD |
		// | ts | TimestampMillisecond | PRI | NO | current_timestamp() | TIMESTAMP |
		// +-------------+------------------------+-----+------+---------------------+---------------+
		main.createNewsArticlesTable(connection);

		String csvPath = "AG_news_samples.csv";
		String csvUrl = "https://raw.githubusercontent.com/openai/openai-cookbook/main/examples/data/AG_news_samples.csv";

		// Download the CSV file.
		main.downloadCsvFile(csvUrl, csvPath);

		// Read the CSV file to columns which contains the title, description.
		List<List<String>> columns = main.readCsvFile(csvPath);

		// Embed the descriptions, convert the vector to string and add it to the
		// columns.
		List<List<Float>> embededDesc = main.embedDescriptions(columns.get(1));
		List<String> embededDescString = main.embedDescriptionsToStr(embededDesc);
		columns.add(embededDescString);

		// Create a Statement
		String prepareStatement = "Insert into news_articles (title, description, embedding) values (?, ?, ?)";
		PreparedStatement statement = connection.prepareStatement(prepareStatement);

		// Insert the data into the news_articles table.
		for (int i = 0; i < columns.get(0).size(); i++) {
			String title = columns.get(0).get(i);
			String description = columns.get(1).get(i);
			String embedding = columns.get(2).get(i);
			main.insertNewsArticle(statement, title, description, embedding);
		}

		// Close the statement and connection.
		statement.close();
		connection.close();

	}

	// Insert the entry into the news_articles table.
	void insertNewsArticle(PreparedStatement statement, String title, String description, String embedding) {
		try {
			statement.setString(1, title);
			statement.setString(2, description);
			statement.setString(3, embedding);
			statement.execute();
		} catch (SQLException e) {
			 e.printStackTrace();
		}
	}

	// Build the insert statement.
	String buildInsertStatement(Statement statement, String title, String description, String embedding) {
		String insertStatement = "INSERT INTO news_articles (title, description, embedding) VALUES ('" + title
				+ "', '"
				+ description + "', " + embedding + ")";
		return insertStatement;
	}

	// Embedding the descriptions.
	List<List<Float>> embedDescriptions(List<String> descriptions) {
		List<List<Float>> embededDesc = new ArrayList<>();
		for (String description : descriptions) {
			embededDesc.add(embedDescription(description));
		}
		return embededDesc;
	}

	// Embedding the description.
	List<Float> embedDescription(String description) {
		// TODO: fix it
		List<Float> embededDesc = new ArrayList<>(768);
		for (int i = 0; i < 768; i++) {
			embededDesc.add((float) i);
		}
		return embededDesc;
	}

	// Convert the vector to string, which is convenient for inserting into the
	// database.
	List<String> embedDescriptionsToStr(List<List<Float>> descriptions) {
		List<String> embededDescString = new ArrayList<>();
		for (List<Float> description : descriptions) {
			String str = "[";
			for (int i = 0; i < description.size(); i++) {
				str += description.get(i);
				if (i < description.size() - 1) {
					str += ",";
				}
			}
			str += "]";
			embededDescString.add(str);
		}
		return embededDescString;
	}

	/// Read the CSV file to columns which contains the title, description.
	List<List<String>> readCsvFile(String filePath) {
		List<List<String>> columns = new ArrayList<>();
		List<String> titles = new ArrayList<>();
		List<String> descriptions = new ArrayList<>();

		try (FileReader reader = new FileReader(filePath);
				CSVParser csvParser = new CSVParser(reader,
						CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
			for (CSVRecord csvRecord : csvParser) {
				titles.add(csvRecord.get("title"));
				descriptions.add(csvRecord.get("description"));
			}
			columns.add(titles);
			columns.add(descriptions);
			System.out.println("CSV file processed successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return columns;
	}

	// Download the CSV file.
	void downloadCsvFile(String cvsFileUrl, String filePath) {
		File file = new File(filePath);

		if (file.exists()) {
			System.out.println("File already exists in the local file system.");
			return;
		}

		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet req = new HttpGet(cvsFileUrl);
			HttpResponse resp = client.execute(req);
			HttpEntity entity = resp.getEntity();

			if (entity == null) {
				System.out.println("Failed to download the file.");
				return;
			}

			try (InputStream inputStream = entity.getContent();
					FileOutputStream outputStream = new FileOutputStream(file)) {
				byte[] buffer = new byte[1024];
				int bytesRead;
				while ((bytesRead = inputStream.read(buffer)) != -1) {
					outputStream.write(buffer, 0, bytesRead);
				}
			}
			System.out.println("File downloaded successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Create the news_articles table in GreptimeDB Edge.
	void createNewsArticlesTable(Connection connection) {
		try {
			Statement statement = connection.createStatement();
			statement.execute(createTableSql());
			System.out.println("Table news_articles is created！");
			statement.close();
		} catch (SQLException e) {
			System.out.println("Failed to create table news_articles.");
			e.printStackTrace();
		}
	}

	// Get the create table(news_articles) SQL.
	String createTableSql() {
		String createTableSQL = "CREATE TABLE IF NOT EXISTS news_articles (" +
				"title STRING FULLTEXT," +
				"description STRING FULLTEXT," +
				"embedding VECTOR(768)," +
				"ts timestamp default current_timestamp()," +
				"PRIMARY KEY(title)," +
				"TIME INDEX(ts)" +
				")";

		return createTableSQL;
	}

}
