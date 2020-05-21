# Final Year Project - Clinical Data Warehouse Schema Design and Implementation.

This is the repo for my FYP. There are two main files within this repo, one is the SQL file which contains the SQL statements which are used to transform the dataset within BigQuery. These statements are also responsible for generating the date and time dimension. The other file is a Python notebook which is a use-case example on how one might query the data warehouse to fetch data into a Pandas dataframe. This dataset can then be used as input for other applications such as data mining.


# Pre-Requisits
First and foremost, before accessing the data, you need to be granted access to it by the team at Physionet. Because it is clinical data, it cannot be shared with anyone else. 

You can find more about getting access over at the dataset website, https://mimic.physionet.org/gettingstarted/access/

You need to complete the training course and then pass their exam to be granted access. 

## Environment Setup
After being granted access to the dataset, 

 1. Sign up with BigQuery using Google Cloud Console
 2. Either download the CSV files from the MIMIC-III dataset onto your local machine and then upload them onto BigQuery or import them from the cloud using the following tutorial: https://mimic.physionet.org/gettingstarted/cloud/
 3. Store all the tables within a dataset called, 'icu'. 
 4. Create a dataset named 'star'. 
 5. Use the SQL script provided to transform the data into the proposed schema. 
 6. Query the dataset using the web UI client or using the Python notebook. 

The altnernate route is by accessing the data warehouse already set up, this is however strictly only for users that already have access to do the data. This can be done using the auth.json file provided in the submission, linking to that within the Python notebook allows you to run queries on the data warehouse. 


