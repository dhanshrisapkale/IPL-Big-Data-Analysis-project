# IPL-Big-Data-Analysis-project

IPL Data Analysis with Apache Spark
This project leverages the power of Apache Spark to perform in-depth data analysis on Indian Premier League (IPL) cricket matches. By processing large datasets of match and ball-by-ball information, this project aims to extract meaningful insights, identify trends, and visualize key performance indicators (KPIs) for teams and players across various IPL seasons.

✨ Features
Robust Data Ingestion: Load IPL match, ball-by-ball, player, player-match, and team data using defined Spark DataFrames schemas.

Data Cleaning & Transformation:

Handle missing values and correct data types.

Filter out specific extra runs (wides, no-balls) for refined analysis.

Calculate running total of runs per over within each innings.

Flag "high impact" balls based on runs scored or wickets taken.

Extract date components (year, month, day) from match dates.

Categorize match outcomes by win margin (High, Medium, Low).

Analyze the direct impact of the toss winner on match outcome.

Normalize player names for consistency.

Categorize players based on batting hand (Left-Handed, Right-Handed).

Identify "Veteran" players based on age.

Calculate "years since debut" for players.

Comprehensive Player & Team Analysis:

Identify top run-scorers per season.

Analyze economical bowlers during powerplay overs.

Examine toss impact on individual matches.

Scalability: Designed to handle increasing volumes of cricket data using Spark's distributed computing.

Visualizations: Generate charts and graphs to represent findings clearly (though the visualization part would typically involve collecting data to a Pandas DataFrame first).

🚀 Technologies Used
Apache Spark (PySpark): The core engine for distributed data processing, utilizing Spark SQL and DataFrame API.

Python: The primary programming language for scripting and analysis.

Pandas: For intermediate data manipulation and preparing data for visualization.

Matplotlib / Seaborn: For generating insightful visualizations (used after .toPandas() operations).

Jupyter Notebook: For interactive development and presenting the analysis steps.

📊 Data Source
The data used in this project is publicly available and consists of several CSV files stored in an S3 bucket: s3://ipl-data-analysis-project/.

Ball_By_Ball.csv: Detailed ball-by-ball records.

Match.csv: Summary information for each IPL match.

Player.csv: Player profiles.

Player_match.csv: Player performance in specific matches.

Team.csv: Team information.

You can find these datasets on platforms like Kaggle (search for "IPL Data" if the link changes).

💻 Setup and Installation
Follow these steps to set up the project locally:

Clone the Repository:

git clone https://github.com/your-username/ipl-data-analysis-spark.git
cd ipl-data-analysis-spark

Install Apache Spark:
Download and install Apache Spark from the official website: Apache Spark Downloads. Follow their documentation for setting up environment variables (SPARK_HOME, PATH).

Create a Python Virtual Environment (Recommended):

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

Install Dependencies:

pip install pyspark pandas matplotlib seaborn jupyter
# If running locally and connecting to S3, you might need to install boto3
# pip install boto3

AWS S3 Access:
Ensure your AWS credentials are configured correctly if you are running this outside of an AWS environment (like EMR or Databricks) where IAM roles handle access. You can configure them via environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) or the AWS CLI.

▶️ Usage
To run the analysis and explore the insights, open the Jupyter Notebook:

Start Jupyter Notebook:

jupyter notebook

Open the Notebook: Navigate to the ipl_analysis.ipynb (or similar name) notebook in your browser.

Run Cells: Execute the cells sequentially. The notebook demonstrates:

Initializing a SparkSession.

Defining precise StructType schemas for each CSV file.

Loading data from the specified S3 paths into Spark DataFrames.

Performing various data cleaning and feature engineering transformations using Spark DataFrame API.

Creating temporary views for SQL-based queries.

Executing specific analytical queries using Spark SQL to derive insights such as:

Top scoring batsmen per season.

Most economical bowlers in powerplay overs.

Toss impact on individual match outcomes.

(Future: Steps for collecting results to Pandas for visualization).

📈 Key Insights & Visualizations (Examples)
This project can answer questions such as:

Which team has the highest win percentage across all seasons? (Bar Chart - Future analysis)

Who are the top batsmen by total runs in each season? (Bar Charts - Query already implemented)

Who are the most economical bowlers during the powerplay overs? (Bar Chart - Query already implemented)

How does the toss decision (batting/fielding first) affect the match outcome across individual matches? (Comparison Chart - Query already implemented)

How has the average first innings score changed over different seasons? (Line Chart - Future analysis)

What is the distribution of dismissals (e.g., caught, bowled, run out)? (Pie Chart - Future analysis)

📁 Project Structure
ipl-data-analysis-spark/
├── data/                       # Local directory for data (if not using S3 directly)
│   ├── matches.csv             # Example local data file
│   └── deliveries.csv          # Example local data file
├── ipl_analysis.ipynb          # Jupyter Notebook with the Spark analysis steps
├── README.md                   # This file
├── requirements.txt            # Python dependencies
└── .gitignore

🤝 Contributing
Contributions are welcome! If you have suggestions for new analyses, improvements to the code, or bug fixes, feel free to:

Fork the repository.

Create a new branch (git checkout -b feature/AmazingFeature).

Commit your changes (git commit -m 'Add some AmazingFeature').

Push to the branch (git push origin feature/AmazingFeature).

Open a Pull Request.


