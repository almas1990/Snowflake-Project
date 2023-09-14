Data Analyst Portfolio Project using SQL, Python, Google Sheets,  I have completed this project with the help of  a blogger i will give credits to him his blog link is below:
techTFQ Blog:: https://techtfq.com/blog/sql-data-analytics-project-data-analyst-portfolio-project-using-sql-python-google-sheets#google_vignette=
In this Data Analyst Portfolio project, we will scrape the Otodom website data using the Bright Data tool and then load the data into the Snowflake database. 

In the Snowflake database, we shall analyze, clean, and transform data and finally build reports to answer some of the most common questions related to the property market in Poland. 
[Snowflake_script_Otodom_Analysis.txt](https://github.com/almas1990/turbo-spork/files/12605064/Snowflake_script_Otodom_Analysis.txt)
[Load_Sample_Dataset_to_SF.txt](https://github.com/almas1990/turbo-spork/files/12605066/Load_Sample_Dataset_to_SF.txt)

Part 1 - Scrape data, Load data and Flatten data using SQL. 
Go to Otodom website and analyse the data for better undersdtanding of the project.

https://www.otodom.pl/ 
Go to Brightdata and download the dataset for the project.

https://brightdata.com/

Export the dataset to snowflake stage as mentioned in techTFQ YouTube video

In order to export Otodom dataset from brightdata to snowflake, you first need to create a account with snowflake using below link:

https://www.snowflake.com/en/

Next, Install SnowSQL (Command line tool for Snowflake):
https://developers.snowflake.com/snowsql/
1) Login to Snowflake:
2) Create database:
2) Create virtual warehouse:
3) Create a table and query it:
4) Create file format object.
Note: STRIP_OUTER_ARRAY = TRUE directs the COPY command to exclude the root brackets ([]) when loading data to the table.
5) Create internal stage
==> After creating the stage, go back to brightdata, and export the dataset to snowflake. Mention the stage name as created above.
==> If you are using sample dataset given by techTFQ in csv format then follow the steps mentioned in file "Load_Sample_Dataset_to_SF.txt" to export the dataset to Snowflake using sample file.
6) Copy into table:
==> Note: The ON_ERROR = 'skip_file' clause specifies what to do when the COPY command encounters errors in the files. In this case, when the command encounters a data error on any of the records in a file, it skips the file. If you do not specify an ON_ERROR clause, the default is abort_statement, which aborts the COPY command on the first error encountered on any of the records in a file.
7) Verify the loaded data:
8) Flatten JSON to table	

Part 2 - Transform data using SQL, Python, and Google Sheets. 

OTODOM_DATA_FLATTEN table will have data taken from Otodom but couple of transformation will be required.

==> Before executing Python scripts, create a virtual environment and install all the required packages as mentioned in file "Python_Prerequisites_Otodom_Analysis.txt".
==> Before executing the below D2 step, you will need to go to google developer console (have an account) and do the following:
       ==> Create a project and select it as current project.
       ==> Go to Librabry and search for "Google Drive API" and enable it.
       ==> Go to Librabry and search for "Google Sheets API" and enable it.
       ==> Go to credentials and create a credential. Choose service account.
       ==> Once service account credential is created, click on it and go to keys and create a new key in JSON format.
       ==> Download the JSON file (generated key). This file is required for python to connect to your google drive to create and work with google sheets.


D1) Transform location coordinates (Latitude and Longitude) into proper address having city, suburb, country etc. 
This can be achieved in Python using "geopy.geocoders". 

So execute the python script "fetch_address_Otodom_Analysis.py". This will take location data from OTODOM_DATA_FLATTEN table and return the proper address for each location and load it into a new table OTODOM_DATA_FLATTEN_ADDRESS.

If you do not want to use Python, then a csv file containing address data is given, please upload csv file data to a new table OTODOM_DATA_FLATTEN_ADDRESS_FULL.



D2) Translate Title from Polish to English language.
This can also be achieved using the same python program as used above and by calling the GoogleTranslator API from Python. However for over 200k records this will fail since 200k is the max limit per day/month.

Alternatively, we can achieve this in Google sheets using the GoogleTranslator API. In Google sheets, if we split the records into multiple files of 10k records each, the API seems to be working.

Please execute the Python script "translate_text_gsheet_Otodom_Analysis.py". This will create multiple (32 files for given full dataset) in the shared google account.
Once the files are created, please wait for 30-60 mins for all the translation to happen within the google sheets.

Then run the next Python script "load_data_gsheet_to_SF_Otodom_Analysis.py" to load the data from google sheets back to Snowflake. This will create a new table in snowflake by name "OTODOM_DATA_FLATTEN_TRANSLATE" which will have the new column "TITLE_ENG" (translated title in English)
Now you have the 3 tables:
       ==> OTODOM_DATA_FLATTEN - Contains original flattened Otodom dataset.
       ==> OTODOM_DATA_FLATTEN_ADDRESS_FULL - Contains address for all locations coordinates.
       ==> OTODOM_DATA_FLATTEN_TRANSLATE - Contains english translated title.


Using the above 3 tables, create a new tables as mentioned below.
This will also create a new column "APARTMENT_FLAG" which can be used to determine if the property ad is for an apartment or for a non apartment (like office space, commercial buildings etc)


Part 3 - Analyse, Clean and build reports using SQL. 

Using the above OTODOM_DATA_TRANSFORMED table, solve the below problems. Answers are given in seperate file "Problems_n_Solutions.txt"


1) What is the average rental price of 1 room, 2 room, 3 room and 4 room apartments in some of the major cities in Poland? 
       Arrange the result such that avg rent for each type fo room is shown in seperate column

2) I want to buy an apartment which is around 90-100 m2 and within a range of 800,000 to 1M, display the suburbs in warsaw where I can find such apartments.       

3) What size of an apartment can I expect with a monthly rent of 3000 to 4000 PLN in different major cities of Poland?

4) What are the most expensive apartments in major cities of Poland? Display the ad title in english along with city, suburb, cost, size.

5) What is the percentage of private & business ads on otodom?

6) What is the avg sale price for apartments within 50-70 m2 area in major cities of Poland?

7) What is the average rental price for apartments in warsaw in different suburbs?
       Categorize the result based on surface area 0-50, 50-100 and over 100.

8) Which are the top 3 most luxurious neighborhoods in Warsaw? Luxurious neighborhoods can be defined as suburbs which has the most no of of apartments costing over 2M in cost.

9) Most small families would be looking for apartment with 40-60 m2 in size. Identify the top 5 most affordable neighborhoods in warsaw.

10) Which suburb in warsaw has the most and least no of private ads?

11) What is the average rental price and sale price in some of the major cities in Poland?

For a couple of the tasks involving the transformation of data, we shall use Python and Google Sheets. 
I am building this project using property listing data taken from the https://www.otodom.pl/ website using the Bright Data platform. However, a similar project can be built using any other dataset of your choice.

ALL CREDITS to techTFQ
techTFQ channel
https://youtu.be/GxmrInUIMAE?si=BTDEgLW9W4_QwhSm
