Ecommerce Real-time Data Processing with Apache Kafka and Spark

## Project Overview
The project demonstrates a real-time data processing pipeline for e-commerce events using Apache Kafka, Apache Cassandra, Apache Spark, and MinIO. The pipeline generates synthetic e-commerce data (clicks, searches, purchases), produces them to Kafka topics, consumes and processes them using Spark, and writes the data to both Cassandra and MinIO.

## Technologies
 - Apache Kafka: Used for real-time streaming and messaging.
 - Apache Cassandra: NoSQL database for storing e-commerce events.
 - Apache Spark: For processing streaming data.
 - MinIO: Object storage service compatible with Amazon S3.
 - Docker: Containerization of services for easy setup and deployment.


## Usage
**Clone the Repository**

   `git clone <https://github.com/khalsz/Ecommerce-end-2-end-data-engineering>`
   `cd <repository-directory>`

## Configuration
 1. **Environment Variables:** Set up the following environment variables in your .env file:
    `MINIO_USERNAME=<your-minio-username>`
    `MINIO_PASSWORD=<your-minio-password>`
    `DB_USERNAME=<your-cassandra-username>`
    `DB_PASSWORD=<your-cassandra-password>`
 2. **Docker:** Ensure Docker and Docker Compose are installed on your system.

## Running the Project
 1. **Build and Start Services:**
    `docker-compose up -d`
 2. **Generate and Produce Data:**
   - Run the data generation script to produce events to Kafka:
     `python path/to/data_generation_script.py`
 3. **Consume and Process Data:**
   - Run the Spark consumer script to process and store data in Cassandra and MinIO:
     `python path/to/spark_consumer_script.py`

 **Stopping the Project**
    `docker-compose down`

## Dependencies
The specific dependencies required for this project are listed in the requirements.txt file. Install them using:
    `pip install -r requirements.txt`

## Contributing
We welcome contributions to improve this project. Please consider creating a pull request on GitHub with your changes and adhering to any project coding style or documentation standards (if applicable).

## Licenses
This project's license depends on the specific libraries used. Refer to the license files of each library used in the project for details.

