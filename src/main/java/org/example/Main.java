package org.example;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

// This demo shows how to store and query vectors in GreptimeDB.
//
// Note: since use Qianwen, so you need to apply an API key of Qianwen from Alibaba Cloud.
// And set environment variable, for example: `export DASHSCOPE_API_KEY=your_api_key`
//
// The process of demo
// 1. Create the news_articles table.
// 2. Download the data file from the internet.(AG_news_samples.csv)
// 3. Read the data file and get the title and description.
// 4. Embed the descriptions via Qianwen from Alibaba Cloud.
// 5. Insert the title, description, and embedding into the news_articles table.
// 6. Query the news_articles table via the vector function vec_dot_product.

public class Main {
	public static void main(String[] args) throws Exception {
		// The default port(mysql) of GreptimeDB Edge is 4002.
		String url = "jdbc:mysql://localhost:4002/public";
		String user = "root";
		String password = "";

		// 1. Load the MySQL JDBC driver.
		Class.forName("com.mysql.cj.jdbc.Driver");

		// 2. Connect to GreptimeDB Edge.
		Connection connection = DriverManager.getConnection(url, user, password);

		Main main = new Main();

		// 3. Create the news_articles table.
		//
                // 
                // +-------------+------------------------+-----+------+---------------------+---------------+
                // |   Column    |          Type          | Key | Null |       Default       | Semantic Type |
                // +-------------+------------------------+-----+------+---------------------+---------------+
                // | title       | String                 | PRI | YES  |                     | TAG           |
                // | description | String                 |     | YES  |                     | FIELD         |
                // | genre       | String                 |     | YES  |                     | FIELD         |
                // | embedding   | Vector(1024)           |     | YES  |                     | FIELD         |
                // | ts          | TimestampMillisecond   | PRI | NO   | current_timestamp() | TIMESTAMP     |
                // +-------------+------------------------+-----+------+---------------------+---------------+
		main.createNewsArticlesTable(connection);

		String csvPath = "AG_news_samples.csv";
		String csvUrl = "https://raw.githubusercontent.com/openai/openai-cookbook/main/examples/data/AG_news_samples.csv";

		// 4. Download the CSV file.
		main.downloadCsvFile(csvUrl, csvPath);

		// 5. Read the CSV file to columns which contains the title, description.
		List<List<String>> columns = main.readCsvFile(csvPath);

		// 6. Embed the descriptions, convert the vector to string and add it to the
		// columns. Default dimension is 1024.
		List<String> description_column = columns.get(1);
		List<List<Double>> embededDesc = main.embedDescriptions(description_column);
		List<String> embededDescString = main.embedDescriptionsToStr(embededDesc);
		columns.add(embededDescString);

		// 7. Create a Statement
		String prepareStatement = "Insert into news_articles (title, description, embedding) values (?, ?, ?)";
		PreparedStatement statement = connection.prepareStatement(prepareStatement);

		// 8. Insert the data into the news_articles table.
		System.out.println("Start inserting data.");
		for (int i = 0; i < columns.get(0).size(); i++) {
			String title = columns.get(0).get(i);
			String description = columns.get(1).get(i);
			String embedding = columns.get(2).get(i);
			main.insertNewsArticle(statement, title, description, embedding);
		}
		System.out.println("Data inserted successfully.");

		// 9. Query the news_articles table and print the result.
		main.queryNewsArticles(connection);

		// 10. Close the statement and connection.
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
	List<List<Double>> embedDescriptions(List<String> descriptions) {
		System.out.println("Start embedding for description! Please wait...");
		List<List<Double>> embeddings = new ArrayList<>(descriptions.size());
		List<String> inputs = new ArrayList<>(10);
		int i = 0;
		for (String description : descriptions) {
			inputs.add(description);
			if (i > 0 && i % 8 == 0) {
				List<List<Double>> result = Qianwen.batchEmbeddingText(inputs);
				for (List<Double> item : result) {
					embeddings.add(item);
				}
				inputs.clear();
			}
			i++;
		}
		if (!inputs.isEmpty()) {
			List<List<Double>> result = Qianwen.batchEmbeddingText(inputs);
			for (List<Double> item : result) {
				embeddings.add(item);
			}
		}
		
		System.out.println("Embedding for description is done.");
		return embeddings;
	}

	// Convert the vector to string, which is convenient for inserting into the
	// database.
	List<String> embedDescriptionsToStr(List<List<Double>> descriptions) {
		List<String> embededDescString = new ArrayList<>(descriptions.size());
		for (List<Double> description : descriptions) {
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

		CSVFormat format = CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true).build();

		try (FileReader reader = new FileReader(filePath);
				CSVParser csvParser = new CSVParser(reader, format)) {
			for (CSVRecord csvRecord : csvParser) {
				titles.add(csvRecord.get("title"));
				descriptions.add(csvRecord.get("description"));
			}
			columns.add(titles);
			columns.add(descriptions);
			System.out.println("CSV file processed successfully.");
		} catch (Exception e) {
			System.out.println("Error processing CSV file.");
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
			int statusCode = resp.getStatusLine().getStatusCode();

			if (statusCode != 200) {
				System.out.println("Failed to download the file. HTTP error code: " + statusCode);
				return;
			}

			HttpEntity entity = resp.getEntity();
			if (entity == null) {
				System.out.println("Failed to download the file. No content.");
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
		} catch (ClientProtocolException e) {
			System.out.println("Protocol error during file download.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("I/O error during file download.");
			e.printStackTrace();
		}
	}

	// Create the news_articles table in GreptimeDB Edge.
	void createNewsArticlesTable(Connection connection) {
		String createTableSQL = createTableSql();
		try (Statement statement = connection.createStatement()) {
			statement.execute(createTableSQL);
			System.out.println("Table news_articles is created!");
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
				"embedding VECTOR(1024)," +
				"ts timestamp default current_timestamp()," +
				"PRIMARY KEY(title)," +
				"TIME INDEX(ts)" +
				")";

		return createTableSQL;
	}

	// Query the news_articles table.
	void queryNewsArticles(Connection connection) {
		String searchQuery = "China Sports";
		List<String> searchQueries = new ArrayList<>();
		searchQueries.add(searchQuery);

		// Get the embedding for the search query
		List<List<Double>> searchEmbeddingList = Qianwen.batchEmbeddingText(searchQueries);
		if (searchEmbeddingList.isEmpty()) {
			System.out.println("Failed to get embedding for the search query.");
			return;
		}
		List<Double> searchEmbedding = searchEmbeddingList.get(0);

		// Convert the embedding to a string format for SQL
		StringBuilder embeddingStr = new StringBuilder("[");
		for (int i = 0; i < searchEmbedding.size(); i++) {
			embeddingStr.append(searchEmbedding.get(i));
			if (i < searchEmbedding.size() - 1) {
				embeddingStr.append(",");
			}
		}
		embeddingStr.append("]");

		// Prepare the SQL query
		String queryStatement = "SELECT title, description, " +
				"vec_dot_product(embedding, ?) AS score " +
				"FROM news_articles " +
				"ORDER BY score DESC " +
				"LIMIT 10";

		System.out.println("Querying news_articles table, sql: " + queryStatement);

		try (PreparedStatement preparedStatement = connection.prepareStatement(queryStatement)) {
			preparedStatement.setString(1, embeddingStr.toString());

			// Execute the query
			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				while (resultSet.next()) {
					String title = resultSet.getString("title");
					String description = resultSet.getString("description");
					double score = resultSet.getDouble("score");
					System.out.println("Title: " + title + ", Description: " + description
							+ ", Score: " + score);
				}
			}
		} catch (SQLException e) {
			System.out.println("Failed to query news_articles.");
			e.printStackTrace();
		}
	}
}
