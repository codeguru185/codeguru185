import json
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StrataQueries").getOrCreate()

# Sample JSON input
input_json = {
    "Year": [2022],
    "Month": [1, 2, 3],
    "Days": [1, 2, 3],
    "Time_intervals": {
        "morning": {"start": "06:00", "end": "09:00"},
        "forenoon": {"start": "09:01", "end": "12:00"},
        "afternoon": {"start": "12:01", "end": "15:00"},
        "evening": {"start": "15:01", "end": "18:00"},
        "late_evening": {"start": "18:01", "end": "21:00"},
        "night": {"start": "21:01", "end": "00:00"},
        "late_night": {"start": "00:01", "end": "03:00"},
        "early_morning": {"start": "03:01", "end": "06:00"}
    },
    "record_percent": 10
}

# Construct Spark SQL queries based on the input JSON and time intervals
def construct_queries(json_data):
    queries = []

    for year in json_data["Year"]:
        for month in json_data["Month"]:
            for day in json_data["Days"]:
                for interval_name, interval_times in json_data["Time_intervals"].items():
                    start_time = interval_times["start"]
                    end_time = interval_times["end"]
                    query = f"""
                        SELECT * FROM t1
                        WHERE year = {year}
                            AND month = {month}
                            AND day = {day}
                            AND time >= '{start_time}' AND time <= '{end_time}'
                        TABLESAMPLE SYSTEM ({json_data["record_percent"]})
                    """
                    queries.append(query)

    return queries

# Execute queries using Spark SQL and write the output to a CSV file
def execute_queries_and_write_csv(queries):
    for index, query in enumerate(queries, start=1):
        result_df = spark.sql(query)
        output_csv_path = f"output_query_{index}.csv"
        result_df.write.csv(output_csv_path, header=True)

# Main function
def main():
    queries = construct_queries(input_json)
    execute_queries_and_write_csv(queries)

# Entry point
if __name__ == "__main__":
    main()
    spark.stop()
