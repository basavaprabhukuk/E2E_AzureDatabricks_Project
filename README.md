# E2E_AzureDatabricks_Project
**How I used Azure Databricks to process and analyze Formula One data from ADLS Gen2**

As a data enthusiast, I wanted to explore the potential of** Azure Databricks**, a unified analytics platform that combines the best of Apache Spark and Microsoft Azure. I decided to use Formula One data as my source, since it is a rich and interesting dataset for anyone interested in sports and racing.

I used the medalion architecture, which consists of three layers: raw, processed, and presentation. I stored the raw data in ADLS Gen2, a scalable and secure cloud storage service. I used Databricks notebooks to consume, filter, clean, and apply minimal transformations to the raw data, and loaded it into the processed layer as parquet format. I learned how to process different types of data, such as CSV, single-line JSON, multiple JSON, and multi-line JSON.

I leveraged Spark SQL to create a database and managed and external tables accordingly. I combined multiple datasets from the ingestion layer, filtered the appropriate results, and selected only the columns I needed. I loaded the data into the presentation layer incrementally, using the Insert Into command with dynamic partition setting enabled. This allowed Spark to identify and drop only the partition column and append the current file date data.

**Some of the key features I learned and did hands-on are:**

**Accessing ADLS Gen2 from Databricks by Service Principal (Azure Active Directory):** Service Principals are similar to user accounts. They can be registered in the Azure Active Directory and assigned permissions required to access the resources in the Azure subscription via role-based access control or RBAC.

**Mounting the ADLS Gen2 in Databricks:** Databricks mounts these storage accounts to DBFS. I specified the credential when the storage was mounted. Once it was mounted, everyone who had access to the workspace could access the data without providing the credentials.

**Accessing secrets from Azure Key Vault:** I created an Azure Key Vault in ADF and created the Secret Scope by adding “secrets/createScope” at the end of the URL Databricks. I then got the secrets in the Databricks notebook using the secret utilities.

**Databricks utilities:** I used the widget utility to pass parameters to the notebook and the notebook workflow utility to invoke one notebook with another and chain them together.
![image](https://github.com/basavaprabhukuk/E2E_AzureDatabricks_Project/assets/96594057/3b916157-2616-404f-8e1f-75e482a9d201)


