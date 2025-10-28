This project is built entirely on Azure Databricks, leveraging the power of Spark Structured Streaming for real-time data ingestion and PySpark for large-scale data transformations. It further incorporates Delta Live Tables to automate the management of Slowly Changing Dimensions (SCDs), ensuring data consistency and reliability. The project concludes with the implementation of dynamic dimensional modeling, delivering well-curated and analytics-ready datasets.

Resources





Azure Services 



Azure Databricks 



Spark Structured Streaming 



Delta Live Tables Delta Lake & Delta Tables 



Databricks SQL Warehouse 



Unity Catalog 



Languages & Tools: Python (PySpark / notebooks) SQL

<img width="1444" height="902" alt="DBDATA" src="https://github.com/user-attachments/assets/617df215-7178-404b-bbd3-f8cfddd2cc47" />


For the detailed Storytelling of this project please visit this link:  https://www.jesseportfolio.co.uk/post/azure-end-to-end-databricks-project
## Creating External Locations
To kickstart the project, I provisioned the Azure Databricks workspace. Since Unity Catalog was already enabled, I proceeded to create external locations for the Medallion Architecture layers — Bronze, Silver, and Gold — along with a source container that holds our ingested data.
<img width="1440" height="721" alt="Screenshot 2025-10-25 at 05 51 52" src="https://github.com/user-attachments/assets/20681df1-1969-432a-85cf-bb7d3ad7eed9" />
<img width="1440" height="721" alt="Screenshot 2025-10-25 at 05 52 29" src="https://github.com/user-attachments/assets/36230a91-4298-4607-a2bd-e97a1d42b088" />
<img width="1440" height="721" alt="Screenshot 2025-10-25 at 05 53 04" src="https://github.com/user-attachments/assets/ff4cf063-f58c-43e4-bd58-bbee4c99162b" />

## Ingestion With Autoloaders(Spark Structured Streaming)
To handle data ingestion from the source to the staging layer, I implemented Spark Structured Streaming. After successfully testing the incremental load for the first table, I built a Job Pipeline to automate and streamline the process—allowing multiple pipelines to run efficiently without any hardcoding.
<img width="1440" height="721" alt="Screenshot 2025-10-25 at 06 52 29" src="https://github.com/user-attachments/assets/b34db7e3-4873-43f1-9b91-bdaaa17818d3" />
Spark Structured Streaming follows the principle of idempotency (exactly-once processing). After successfully running the initial streaming load into the staging layer, I added a new dataset, which was ingested seamlessly. Since the previously processed data was already stored in the data lake, this setup provided the perfect foundation for implementing incremental load procedures in the project.
<img width="1440" height="721" alt="Screenshot 2025-10-25 at 06 53 01" src="https://github.com/user-attachments/assets/8ef805cf-e8f9-43ff-a9e8-4754ef37978e" />

<img width="766" height="70" alt="Screenshot 2025-10-25 at 06 53 27" src="https://github.com/user-attachments/assets/f670eea5-67f0-4bc7-9f27-a5c413245f0d" />



<img width="766" height="244" alt="Screenshot 2025-10-25 at 06 53 52" src="https://github.com/user-attachments/assets/4eeffce8-07b0-48a9-8cf0-c73269a8120d" />



<img width="766" height="424" alt="Screenshot 2025-10-25 at 06 54 16" src="https://github.com/user-attachments/assets/70409a18-b8e9-4138-a28f-181ef82630c4" />


Initially, the dataset contained 1,000 rows. After running the streaming process, the new data was successfully ingested, bringing the total to 1,300 rows. Once this was confirmed, I proceeded to create the Jobs Pipeline to automate the workflow.

## Job Pipeline Run(Successful)

<img width="1440" height="726" alt="Screenshot 2025-10-25 at 07 44 39" src="https://github.com/user-attachments/assets/87690380-fa92-4c8f-8dea-474de4c7ea1c" />


<img width="1440" height="726" alt="Screenshot 2025-10-25 at 07 44 49" src="https://github.com/user-attachments/assets/a9b6ba4e-1c8d-42e8-8167-72c93bf853bd" />

The Job Pipeline ran successfully. To validate the effectiveness of Spark Structured Streaming, I added new datasets to the data lake and reran the pipeline.


<img width="1440" height="716" alt="Screenshot 2025-10-25 at 07 49 16" src="https://github.com/user-attachments/assets/a4ac38cd-e83f-4230-9692-27b7955286fc" />



<img width="1440" height="725" alt="Screenshot 2025-10-25 at 07 53 26" src="https://github.com/user-attachments/assets/c4108792-9bb3-4ee5-b667-5cedbab4d032" />


<img width="1440" height="725" alt="Screenshot 2025-10-25 at 07 53 48" src="https://github.com/user-attachments/assets/004c3d8e-0512-49cb-a359-e08167bd9646" />



<img width="1440" height="725" alt="Screenshot 2025-10-25 at 07 54 02" src="https://github.com/user-attachments/assets/8feb5e59-9b5b-42db-ba4b-1e56cf57d70d" />


<img width="757" height="46" alt="Screenshot 2025-10-25 at 07 52 13" src="https://github.com/user-attachments/assets/e9e4ad8c-302d-43ad-8a3b-13a8174102e4" />


<img width="757" height="46" alt="Screenshot 2025-10-25 at 07 51 32" src="https://github.com/user-attachments/assets/83a860ac-a835-4d62-a5f4-b6291f5e02c5" />

<img width="757" height="46" alt="Screenshot 2025-10-25 at 07 52 59" src="https://github.com/user-attachments/assets/e5fd42d7-cfaa-4428-9a14-63697a56737c" />

As shown in the images with the rows, these are rows from the first run below are images from the second runs and with rows to show its Incremental Load success.



<img width="1440" height="725" alt="Screenshot 2025-10-25 at 07 54 44" src="https://github.com/user-attachments/assets/cf298d92-15af-4815-953d-06609d434180" />

<img width="1440" height="725" alt="Screenshot 2025-10-25 at 07 57 15" src="https://github.com/user-attachments/assets/cfe30557-aa5b-42e5-b193-43fffa81b6b6" />

<img width="1440" height="725" alt="Screenshot 2025-10-25 at 07 57 27" src="https://github.com/user-attachments/assets/a0bde73e-3491-4d31-9e11-d058d97a6b1f" />




<img width="763" height="48" alt="Screenshot 2025-10-25 at 07 58 43" src="https://github.com/user-attachments/assets/f1da7c5e-27cf-40b2-9c7c-b4cc68add4b2" />




<img width="763" height="48" alt="Screenshot 2025-10-25 at 07 58 57" src="https://github.com/user-attachments/assets/1dddfb61-26b3-4e3f-86df-61b817f63752" />

<img width="765" height="53" alt="Screenshot 2025-10-27 at 13 02 38" src="https://github.com/user-attachments/assets/d48d3735-59ea-4d6d-9904-c8aba7196a5a" />


The Job Pipeline and Incremental Load was overwritten and successfully stored in the Staging Layer thus concluding the first phase of our Databricks End To End Project
<img width="1440" height="726" alt="Screenshot 2025-10-25 at 07 47 56" src="https://github.com/user-attachments/assets/fce7dcad-71a5-4b00-a551-b03c070ddcfc" />



## Phase 2 (Transformation And Storing Finalising Enriched Datasets With DLT)

For the Silver layer, I transformed the ingested data from the staging layer and stored it as a Delta Live Table, while defining table expectations and automating Slowly Changing Dimensions type 1 (UPSERT) for our enriched datasets.





<img width="1440" height="714" alt="Screenshot 2025-10-25 at 08 39 02" src="https://github.com/user-attachments/assets/d5f95947-6a87-4b9d-bcda-811dcbf6d08b" />
I initially stored the Delta Live Table (DLT) pipeline source code as notebooks before converting them to Python files. This approach allowed me to validate transformations and make necessary changes before deploying the pipeline.



<img width="1440" height="714" alt="Screenshot 2025-10-25 at 09 21 36" src="https://github.com/user-attachments/assets/38b2f1a4-222e-4ace-9f53-ed979586c197" />





<img width="1440" height="714" alt="Screenshot 2025-10-25 at 10 08 58" src="https://github.com/user-attachments/assets/b62178f3-fedc-490f-b9d4-25218213045a" />

<img width="1440" height="714" alt="Screenshot 2025-10-25 at 09 21 44" src="https://github.com/user-attachments/assets/e13042d5-d069-4878-a64e-6d536f0d71e8" />


<img width="1440" height="714" alt="Screenshot 2025-10-25 at 11 21 42" src="https://github.com/user-attachments/assets/cdd3f8d7-beaa-4fd1-836c-8370f1e86ede" />




<img width="1440" height="714" alt="Screenshot 2025-10-25 at 10 59 23" src="https://github.com/user-attachments/assets/9b763a90-c747-48b6-98d8-87f1119105e0" />


<img width="1440" height="714" alt="Screenshot 2025-10-25 at 10 59 43" src="https://github.com/user-attachments/assets/7b722a07-b8d3-45ae-9869-ccb39caaf884" />


<img width="1440" height="714" alt="Screenshot 2025-10-27 at 13 00 28" src="https://github.com/user-attachments/assets/0579978b-2955-496b-9f6d-aa7484ca141d" />


<img width="1440" height="714" alt="Screenshot 2025-10-25 at 10 59 14" src="https://github.com/user-attachments/assets/96bdcf77-ac3a-4850-b66c-a17202ce2129" />


After the completion of our DLT Table I wrote my data to the Data Lake Enrichment Container

<img width="1440" height="714" alt="Screenshot 2025-10-25 at 10 53 50" src="https://github.com/user-attachments/assets/a1774a0b-3191-4279-ac78-5d73b50c4fa8" />



<img width="1440" height="714" alt="Screenshot 2025-10-25 at 11 21 52" src="https://github.com/user-attachments/assets/87eb7100-6a9d-4076-a88d-e22003ec00db" />


## Phase 3 (Star Schema & Dynamic Dimensional Modelling)


For the curated Gold layer, I implemented a Dynamic Dimensional Model for our tables — Passengers, Airports, and Flights — functioning similarly to an SCD Builder. I initially tested the model with the Airports data, which then made it straightforward to dimensionally model the remaining tables and apply UPSERT operations.

<img width="1440" height="718" alt="Screenshot 2025-10-28 at 12 15 46" src="https://github.com/user-attachments/assets/4baee098-333f-43b7-b779-e90c0ff1b4b3" />



<img width="1440" height="719" alt="Screenshot 2025-10-27 at 13 06 31" src="https://github.com/user-attachments/assets/1eb9b718-e2aa-469e-b5e9-e1a651d359e9" />




<img width="1440" height="719" alt="Screenshot 2025-10-27 at 13 08 19" src="https://github.com/user-attachments/assets/08a4820a-3531-4bbe-95b2-a0e2096060cc" />

<img width="1440" height="718" alt="Screenshot 2025-10-28 at 12 16 28" src="https://github.com/user-attachments/assets/2847ce28-c272-41da-b978-c0c06bad40ba" />



Dynamic Notebook WorkFlow Video Available here ->  https://www.jesseportfolio.co.uk/post/azure-end-to-end-databricks-project



After successfully completing the Dimensional Tables, I created the Fact Table and validated it to ensure there were no duplicates.

<img width="1440" height="719" alt="Screenshot 2025-10-27 at 13 59 01" src="https://github.com/user-attachments/assets/581237d7-94fd-444c-a81b-b07e4284d99d" />





<img width="1440" height="719" alt="Screenshot 2025-10-27 at 14 12 52" src="https://github.com/user-attachments/assets/8660479f-ac33-4412-961f-32ad994b77fb" />



## DATABRICKS SQL DATA WAREHOUSE

With the final stage of our curated datasets complete our data is loaded succesfully to Databricks SQL Data Warehouse, I validated data authenticity by querying them in SQL Data Warehouse, which returned the curated datasets seamlessly.
<img width="1440" height="719" alt="Screenshot 2025-10-27 at 14 15 17" src="https://github.com/user-attachments/assets/f31b982c-1bbf-4c9c-9928-be4d289d4e16" />

<img width="1440" height="719" alt="Screenshot 2025-10-27 at 15 06 24" src="https://github.com/user-attachments/assets/54693198-4e4e-4581-a413-9bf928d6c3c2" />

<img width="1440" height="719" alt="Screenshot 2025-10-27 at 15 07 05" src="https://github.com/user-attachments/assets/d4cedda5-983f-492e-bd4d-26ee965579ec" />
<img width="1440" height="719" alt="Screenshot 2025-10-27 at 15 07 26" src="https://github.com/user-attachments/assets/c05a7248-16cc-4356-b120-f00ce95b69b0" />
<img width="1440" height="719" alt="Screenshot 2025-10-27 at 15 08 47" src="https://github.com/user-attachments/assets/fab5fbda-29a8-4162-b447-aa9714324d82" />

<img width="555" height="538" alt="Screenshot 2025-10-27 at 15 12 28" src="https://github.com/user-attachments/assets/d3db4a1d-be2f-451d-9a6d-7a098bd36183" />
<img width="555" height="538" alt="Screenshot 2025-10-27 at 15 12 46" src="https://github.com/user-attachments/assets/e9673a98-b3bf-4832-a5d3-2f9684a7adda" />

<img width="1440" height="719" alt="Screenshot 2025-10-27 at 15 06 52" src="https://github.com/user-attachments/assets/3d08fd00-0471-43ce-ae9b-57d565973fda" />


## LOADING TO SYNAPSE WAREHOUSE

Once our curated and enriched datasets were stored in Databricks SQL Warehouse, I proceeded to store the data in the Synapse Workspace using OPENROWSET, creating views, and finally setting up external tables.

<img width="1440" height="718" alt="Screenshot 2025-10-28 at 09 36 28" src="https://github.com/user-attachments/assets/0812f62a-b8d2-4f18-8132-c283f8b62c7b" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 09 39 03" src="https://github.com/user-attachments/assets/fe2cd2dd-f0ba-4d60-9ce4-85ed80ada1cb" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 09 55 10" src="https://github.com/user-attachments/assets/a5c410a6-630a-4c06-81ee-06edf86707e5" />

<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 18 25" src="https://github.com/user-attachments/assets/e17685c7-3c6e-4001-b928-85f24270a337" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 18 50" src="https://github.com/user-attachments/assets/20e76c08-b20d-49dd-aae6-a0d18f02cf65" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 19 40" src="https://github.com/user-attachments/assets/4e7fa4d9-c7ff-48fd-85d4-cabe97daeddc" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 22 26" src="https://github.com/user-attachments/assets/1ef1ecef-6ac3-4188-a910-09cd848c9058" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 38 01" src="https://github.com/user-attachments/assets/8925bd78-19e0-4b23-89c1-bb57db134c2e" />

<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 41 11" src="https://github.com/user-attachments/assets/effc35e8-be73-43ea-b588-baf7501e4890" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 48 14" src="https://github.com/user-attachments/assets/e1ea11c3-9d73-4932-bc80-0bb3cdf85a44" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 52 51" src="https://github.com/user-attachments/assets/3ff0d51a-8ce1-47a1-aad4-34b46618a9a7" />
<img width="1440" height="718" alt="Screenshot 2025-10-28 at 10 55 36" src="https://github.com/user-attachments/assets/343adec3-339a-4ce2-ad3d-ab6b573086f9" />

Lessons To Take From It

*. Although dynamic notebook is amazing and without a doubt I will always reuse it for creating my dimension tables, DLT is still a big player for its automation and will be considered for my arsenal











