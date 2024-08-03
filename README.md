# Covid-19 Deaths:

**Situation**

During the Covid-19 pandemic, there was an urgent need to understand the impact of the virus, particularly in terms of deaths across different demographics and regions. Accurate and timely analysis was critical for policymakers and healthcare providers to make informed decisions.

**Tasks**

Clean and prepare the Covid-19 Deaths data using Microsoft SQL Server

Perform exploratory data analysis using various SQL queries

Calculate key metrics such as death percentages and infection rates

Visualize the analyzed data using Tableau Public

Derive insights about global trends in Covid-19 deaths and vaccinations

**Actions**

1.	Data Cleaning and Preparation with SQL: 

Joins: Combined data from multiple tables to ensure a comprehensive dataset.

Common Table Expressions (CTEs) and Temp Tables: Used to organize and structure complex queries for readability and performance.

Window Functions: Applied for calculations over a specified range of data, such as running totals and moving averages.

Aggregate Functions: Used to summarize data, including counts, averages, and totals.

Creating Views: Established views for simplified data access and to streamline the Tableau connection.

Converting Data Types: Ensured data was in the correct format for analysis, such as converting date strings to date types.

2.	Data Visualization with Tableau: 

Data Connection: Connected Tableau to the SQL views for dynamic data access.

Visualizations: Created various charts and graphs, including line charts for trends, heat maps for geographic distribution, and bar charts for demographic breakdowns.

Dashboards: Compiled the visualizations into interactive dashboards, enabling users to filter and drill down into the data.

**Results**

1. Overview

Total Cases: 2,000,000

Total Deaths: 50,000

Mortality Rate: 2.5%

2. Trend Analysis

New Cases: A significant peak observed in January 2021, with a 7-day moving average showing a decline starting in February 2021.

New Deaths: Mirrored the trend in new cases, peaking slightly later in January 2021.

3. Geographic Distribution

Heat Maps: Highest case densities in urban areas, particularly in New York, California, and Texas.

Regional Comparisons: California had the highest total cases, while New York had the highest death rate.

4. Demographic Breakdown

Age Distribution: The highest number of deaths in the 65+ age group.

Gender Distribution: Slightly more cases in males, but higher death rates in males compared to females.

Ethnicity/Race: Disproportionate impact on Hispanic and Black communities.

5. Vaccination Data

Vaccination Rates: Rapid increase in vaccination rates starting in January 2021, reaching 70% by mid-2021.

Impact on Case/Death Rates: Significant decline in both cases and deaths correlating with the increase in vaccination rates.

6. Healthcare Impact

Hospitalizations: Peaks in hospitalizations aligning with peaks in new cases.

ICU Admissions: ICU admissions spiked in January 2021, causing strain on healthcare systems.

7. Comparative Analysis

Policy Impact: Regions with stricter public health measures saw faster declines in new cases and deaths.

**Key outcomes:**

Demonstrated proficiency in SQL, particularly in creating views and temporary tables

Showcased ability to transform raw data into actionable insights through effective visualization

Deepened understanding of global health trends during the Covid-19 pandemic

Improved skills in data cleaning, analysis, and visualization using SQL Server and Tableau Public

Created a valuable resource for understanding the impact of Covid-19 across different continents

This project not only enhanced my technical skills in SQL and data visualization but also provided meaningful insights into a critical global health issue, demonstrating my ability to handle and analyze complex, real-world datasets.

# Layoffs from Around the World

In this project I used MySQL to clean Data for Layoffs from Around the World data set and was able to do Exploratory Data Analysis with the clean data.

**Data Cleaning:**

This project focuses on data cleaning in MySQL, demonstrated how to prepare raw data for analysis by fixing issues in a dataset about global layoffs.

Highlights
•	📊 Database Creation: Started by creating a MySQL database to house my dataset.

•	📈 Real Dataset Import: Imported a dataset containing global layoffs data including company and location details.

•	🔧 Data Cleaning Techniques: Removed duplicates, standardize data formats, and handle null values.

•	🗃️ Row and Column Management: Removed unnecessary rows and columns to streamline the dataset.

•	💻 SQL Query Demonstration: Utilized SQL queries to manipulate and clean the dataset effectively.

•	✅ Data Integrity Assurance: Implemented staging tables to ensure the integrity of my cleaned data.

Key Insights

•	🛠️ Understanding Data Cleaning: Data cleaning is vital for accurate analysis, as it directly affects the quality of insights derived from datasets.

•	🔍 Identifying Duplicates: Using row numbers in SQL is an effective method to identify and remove duplicate entries, ensuring data uniqueness.

•	⚖️ Standardizing Data: Standardizing industry names and trimming white spaces are crucial for consistency, making it easier to analyze and compare data.

•	❓ Handling Null Values: Properly addressing null values is essential to avoid skewed results and ensure comprehensive data analysis.

•	🏗️ Staging Tables for Integrity: Using staging tables during the cleaning process helps maintain data integrity and allows for easier rollback if needed.

•	🗑️ Eliminating Unnecessary Data: Removing irrelevant rows and columns not only simplifies the dataset but also enhances performance during analysis.

**Exploratory Data Analysis:**

This project focuses on exploratory data analysis (EDA) of a cleaned dataset, examining layoffs by companies, industries, and trends over time.

Highlights

📊 Focused on exploratory data analysis (EDA) using a cleaned dataset.

🔍 Discovered insights without a specific agenda, exploring various data facets.

📈 Analyzed total layoffs, identifying peak layoffs from different companies.

🏢 Investigated layoffs across industries, revealing the hardest-hit sectors.

🌍 Examined layoffs by country, highlighting significant job losses globally.

📅 Explored trends over time, particularly through rolling totals and year comparisons.

📈 Emphasized the importance of data cleaning and visualization in EDA.

Key Insights

📉 The dataset reveals significant layoffs during the pandemic, with tech companies like Amazon and Google leading.

🏭 Industries such as retail and consumer services faced the highest layoffs, likely due to pandemic-related disruptions.

🌐 The U.S. dominated the layoffs data, indicating larger corporate layoffs compared to other countries.

🗓️ Analyzing layoffs by year shows a progressive increase, with 2022 being the worst year for layoffs recorded.

🔄 Rolling totals provide a clearer picture of layoffs over time, helping visualize trends and peaks.

📊 Multiple CTEs (Common Table Expressions) are useful for complex queries, allowing for detailed insights by year and company.

