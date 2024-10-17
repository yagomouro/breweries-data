# Breweries Case
<br/>

## Table of Contents
- [How to Run the Project](#how-to-run-the-project)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Azure Storage Connection](#azure-storage-connection)
  - [Folder Structure](#folder-structure)
- [About the project](#about-the-project)
  - [API](#api)
  - [Orchestration Tool](#orchestration-tool)
  - [Language and Data Transformation](#language-and-data-transformation)
  - [Containerization](#containerization)
  - [Data Lake Architecture](#data-lake-architecture)
    - [Bronze Layer](#bronze-layer)
    - [Silver Layer](#silver-layer)
    - [Gold Layer](#gold-layer)
  - [Monitoring and Alerting](#monitoring-and-alerting)
  - [Trade-offs](#trade-offs)
  - [Extra Step: Data Visualization](#extra-step-data-visualization)

## How to Run the Project

### Prerequisites
Before running the project, ensure you have the following configured:

- [Python 3.11](https://www.python.org/downloads/)
- [Apache Airflow](https://airflow.apache.org/)
- [Docker](https://www.docker.com/)
- [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/services/storage/data-lake-storage/)
- [Apache Spark](https://spark.apache.org/)


## Installation
Clone the repository:
```bash
git clone https://github.com/yagomouro/breweries-data-solution.git
cd breweries-data-solution
```

Set up Apache Airflow running Docker:
```bash
docker compose up
```

## Azure Storage Connection

To run this project, you need to configure an Azure Storage Data Lake Gen 2, which will be used to store the extracted and transformed data.

1. **Create an Azure Data Lake Storage Gen2 (ADLS Gen2)**:
   - In your Azure portal, create an ADLS Gen2.
   - Inside your ADLS Gen2, create three containers: bronze-layer, silver-layer and gold-layer.


2. **Accessing the Airflow UI**:
   - Start your Airflow instance and open the Airflow UI by navigating to [http://localhost:8081](http://localhost:8081) in your browser.
   - Login with the default credentials:
     - **Username**: `airflow`
     - **Password**: `airflow`

3. **Azure Storage Connection String**:
   - After creating the storage account, retrieve the *Connection String*.
   - You will add this connection string directly in the Airflow UI by following these steps:

     Go to `Admin > Variables > Add a new Record`.

     - In the **Key** field, enter: `AZURE_STORAGE_CONNECTION_STRING`.
     - In the **Val** field, paste the connection string you retrieved from your Azure Data Lake Storage (ADLS).



## Folder Structure

```
└── 📁breweries-data-solution          
    └── 📁airflow                      
        └── 📁config                    
            └── airflow_connections.json   # Configuration for external connections in Airflow
        └── 📁dags                     
            └── orchestrator_dag.py       # Main DAG file to orchestrate the entire pipeline
        └── 📁jobs                     
            └── bronze_layer.py           # Script responsible for ingesting raw data into the Bronze layer
            └── gold_layer.py             # Script for transforming data into aggregated metrics in the Gold layer
            └── silver_layer.py           # Script that handles data transformations for the Silver layer
        └── 📁plugins                  
            └── spark_manager.py          # Manages Apache Spark sessions for distributed data processing
            └── storage_client.py         # Handles communication with Azure Data Lake Gen 2
        └── 📁tests                     
            └── test_bronze_layer.ipynb   # Jupyter Notebook for testing the ingestion of data into the Bronze layer
            └── test_gold_layer.ipynb     # Jupyter Notebook for testing the data aggregation in the Gold layer
            └── test_silver_layer.ipynb   # Jupyter Notebook for testing data transformation in the Silver layer
    └── 📁dataviz                       
        └── BreweriesCase.mp4            # Video demo showcasing the solution
        └── BreweriesCase.pbix           # Power BI report for visualizing insights from the breweries data
    └── 📁logs                          # Logs for Airflow
    └── .env.example                     # Example environment file with variable settings to configure the project
    └── .gitignore                       # Specifies files and folders to be ignored by Git
    └── docker-compose.yaml              # Docker Compose file to define services for running the project in containers
    └── Dockerfile                       # Dockerfile to containerize the application and its dependencies
    └── README.md                        # Readme file
    └── requirements.txt                 # List of Python dependencies required to run the project

```

<br />
<br />

# About the project

## API

In this project, I implemented API integration using the `APIClient` class to interact with the **Open Brewery DB**, which provides some data of breweries, including names, locations, and types.

The `get_data()` method in the `APIClient` class fetches brewery data using the `HttpOperator` from **Apache Airflow** to send a GET request to the API endpoint. Upon successful retrieval, it logs a confirmation message. If there are issues during the API call, the error is logged, and an exception is raised to handle failures gracefully.

## Orchestration Tool

I chose **Apache Airflow** for orchestrating the data pipeline because of its flexibility and robust features. It allows for easy task scheduling, efficient retries, and error management.

I created Directed Acyclic Graphs (DAGs) to define the sequence of operations for data extraction, transformation, and loading into the data lake. This structured approach visualizes dependencies and simplifies debugging, ensuring a reliable workflow that delivers timely insights from the brewery data. <br /><br />

![airflow_dags](https://github.com/user-attachments/assets/d0885bc0-f1b7-4d15-9742-4b093de2d187)



## Language and Data Transformation
For data ingestion and transformation, I used PySpark to efficiently read and process the API data. Its capability to handle large datasets made it ideal, allowing to manipulate data easily with the DataFrame API through filtering, aggregating, and reshaping.

Due to limitations in connecting PySpark directly to Azure Data Lake, I chose to store the transformed data in Parquet format instead of Delta Lake. While Delta Lake offers features like ACID transactions, Parquet provides efficient columnar storage and optimized querying, keeping the data well-structured for future analysis.

After the transformation process was complete, the final dataset was saved using Python, which facilitated a seamless integration with the Azure environment.
<br /><br />

![flow](https://github.com/user-attachments/assets/14204ead-9eef-460a-963f-7ee6eef9cf40)


## Containerization
In this project, I used **Docker** to achieve efficient containerization, making deployment and management of services a breeze. I created a custom **Dockerfile** to set up an Airflow container that includes all the essential components, like Java and Apache Spark, which are crucial for data processing and orchestration.

Using Docker Compose, the architecture was designed with three main services: a Spark master, a Spark worker, and the Airflow instance. The Spark master manages cluster resources, while the worker executes tasks to ensure smooth data processing.

The Airflow service was configured to initialize the database, establish connections, and run the web server and scheduler in the background. This containerized approach enhances scalability and simplifies data pipeline management, enabling effective orchestration of data workflows. <br />


![docker](https://github.com/user-attachments/assets/eb436c97-a8b1-48fa-ac67-8c5bb7197c36)

## Data Lake Architecture
The architecture of the Data Lake is built to utilize Azure Cloud Services for effective data management and accessibility. A dedicated Azure Resource Group hosts the **Azure Data Lake Storage Gen2**, providing a scalable and secure environment for storing large volumes of data.

This architecture follows a layered approach, consisting of three primary layers: Bronze, Silver, and Gold. These layers, also known as Raw, Trusted, and Refined, facilitate a systematic method for data processing. <br /><br />

![datalake](https://github.com/user-attachments/assets/d0cb4096-a5e7-4804-b390-5ecf999544a9)


### Bronze Layer
In the Bronze Layer, I ingested raw data from the Open Brewery DB API by developing the `APIClient` class, which utilizes an Airflow HTTP operator to retrieve the data in JSON format. 

The `BronzeLayerProcessor` class then processes this data and uploads it to **Azure Data Lake Storage Gen2**, storing the raw data in the 'bronze-layer' container as breweries_data.json. This ensures easy access for further processing in the Silver and Gold layers, effectively capturing unprocessed data as a solid foundation for future transformations.

### Silver Layer
In the Silver Layer, I transformed the raw data from the Bronze Layer into a clean and structured format using the `SilverLayerProcessor` class with a Spark-based pipeline. This pipeline reads the raw JSON data from Azure Data Lake, removes duplicates, and partitions the data by state. 

The processed data is saved as Parquet files organized by state in the 'silver-layer' container of Azure Storage. This cleaned and structured data is now ready for further enrichment and analysis in the Gold Layer.

### Gold Layer
In the Gold Layer, I extracted valuable insights from the clean data obtained in the Silver Layer by building the `GoldLayerProcessor` class, which employs a Spark-based pipeline to read the processed Parquet files, ensuring data consistency with a structured schema. 

The pipeline aggregates the brewery data by country, state, city, and brewery type, calculating the total number of breweries in each group. This aggregation helps identify regions with the highest brewery concentrations, providing a clearer view of the industry landscape. The final aggregated data is saved as Parquet and uploaded to the 'gold-layer' container in Azure Storage, making it ready for advanced analytics and reporting.
<br /><br />

By utilizing **Azure's** capabilities, the Data Lake architecture ensures that data is managed effectively across its lifecycle, providing a solid foundation for future analytics and reporting.

## Monitoring and Alerting
To ensure the reliability of the data processing pipeline, I implemented monitoring and alerting by creating test files for each processor to verify their functionalities. This approach allows for consistent checks on each layer's operations.

I also integrated `try-except` blocks throughout the code to handle potential errors gracefully. This ensures that if issues arise during execution—such as API call failures or data processing errors—the system can respond appropriately and provide clear feedback.

## Trade-offs
**PySpark** for Data Processing: The trade-off for selecting PySpark is its efficiency in handling distributed data processing. While it effectively manages large datasets, it has a significant learning curve and can complicate debugging for those unfamiliar with Spark.

**Azure Data Lake** for Storage: The trade-off for opting for Azure Data Lake Storage is its ability to support large volumes of data and offer scalability. However, this may incur higher costs and require more complex data governance than traditional databases.

**Docker** for Containerization: The trade-off for implementing Docker is its ability to provide consistent application environments. While it simplifies dependency management and enhances scalability, it adds complexity in managing containers and orchestration.

## Extra Step: Data Visualization
In the broader context of data engineering, a common question arises: **Why invest time and resources into complex data transformation processes?** The answer lies in the ability of these processes to turn raw data into actionable insights that can drive business decisions. By systematically transforming and refining data, we enable organizations to extract meaningful information that can impact strategy, operations, and growth.

As an example of the value this can provide, I created a data analysis dashboard as an extra step in this project. Using the cleaned and structured data extracted from the Silver Layer, I built a dashboard in Power BI connected directly to the Azure Data Lake Storage Gen2. This dashboard provides insights like brewery distribution by state and type, visualized through interactive charts and maps, which demonstrates how well-organized data can be seamlessly leveraged for business intelligence.

You can access the file in `dataviz > BreweriesCase.pbix`, or watch the video demonstration. 
<br /><br />

**Don't forget to turn on the sound for the best experience! :)**




https://github.com/user-attachments/assets/96ae7af7-558f-450c-9a1f-f70595cd73fa



