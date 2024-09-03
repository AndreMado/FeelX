# Real-Time Sentiment Analysis with Apache Kafka, PySpark, and Airflow

This repository contains a project focused on real-time sentiment analysis using Twitter data. The project integrates various Big Data tools to create a robust and scalable pipeline that processes tweets, analyzes their sentiment (positive, negative, neutral), and visualizes the results in real-time.

## Key Components:
- **PySpark**: Processes the incoming tweets and applies sentiment analysis using machine learning models.
- **Apache Airflow**: Orchestrates the workflow, ensuring smooth execution of all processes.
- **Cassandra**: Stores the processed sentiment analysis results in a distributed NoSQL database.
- **Grafana**: Provides real-time visualization of the sentiment analysis results.

## Objectives:
1. Ingest tweets in real-time from the Twitter API using Apache Kafka.
2. Process and analyze the sentiment of the tweets using PySpark.
3. Automate and manage the data pipeline with Apache Airflow.
4. Store the analysis results in Cassandra for further exploration.
5. Visualize the results in real-time using Grafana.

## Project Structure:
- **`main` branch**: Stable and production-ready code.
- **`development` branch**: Active development happens here.

## How to Contribute:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m 'Add some feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a Pull Request.

Feel free to explore the code and contribute to the project!

## Contact
For any inquiries or questions, feel free to open an issue or contact the repository maintainers.

