import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import os
import warnings
from dotenv import load_dotenv

# Ignore UserWarning warnings.
warnings.simplefilter(action='ignore', category=UserWarning)

# Load environment variables from .env file
load_dotenv()

def establish_connection():
    """
    Establishes a database connection using the environment variables.
    
    Returns:
        conn (psycopg2.extensions.connection): A database connection object.
    """
    dbname = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT")
    
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except Exception as e:
        print(f"Error establishing database connection: {e}")
        return None 

def fetch_companies_from_db(conn):
    """
    Fetch companies brought by the partnerships team with their activation dates.
    
    Args:
        conn (psycopg2.extensions.connection): A database connection object.
    
    Returns:
        df_companies (pd.DataFrame): A dataframe containing company IDs and their activation dates.
    """

    try:
            # Consult activation dates for companies brought by the associaton team.
            query = """
            WITH daily_conversations AS (
                SELECT
                    account_id,
                    date,
                    SUM(total) AS daily_total
                FROM
                    conversations
                GROUP BY
                    account_id, date
            ),

            three_day_conversations AS (
                -- Calcula el total de conversaciones en 3 días seguidos
                SELECT
                    dc1.account_id,
                    dc1.date,
                    dc1.daily_total +
                    COALESCE(dc2.daily_total, 0) +
                    COALESCE(dc3.daily_total, 0) AS total_3_day
                FROM
                    daily_conversations dc1
                LEFT JOIN daily_conversations dc2 ON dc1.account_id = dc2.account_id AND dc2.date = dc1.date - INTERVAL '1 day'
                LEFT JOIN daily_conversations dc3 ON dc1.account_id = dc3.account_id AND dc3.date = dc1.date - INTERVAL '2 days'
                WHERE
                    dc1.daily_total + COALESCE(dc2.daily_total, 0) + COALESCE(dc3.daily_total, 0) >= 350
            ),

            first_activation AS (
                SELECT
                    account_id,
                    MIN(date) AS activation_date
                FROM
                    three_day_conversations
                GROUP BY
                    account_id
            )

            SELECT
                c.id AS company_id,
                c.name AS company_name,
                f.activation_date
            FROM
                company c
            LEFT JOIN
                first_activation f
            ON
                c.id = f.account_id
            WHERE
                c.associated_partner IS NOT NULL AND c.associated_partner != '';

            """

            # Extract the activation dates and names of the companies brought by the alliance team.
            df_companies = pd.read_sql_query(query, conn)
        
            # Returns DataFrame of companies and activation dates
            return df_companies
        
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return None
    
def fetch_successful_conversations(df_companies, conn):
    """
    Fetch the date when each company crosses the threshold of 500 successful conversations within the 2-month period from activation.
    
    Args:
        df_companies (pd.DataFrame): DataFrame containing company IDs and their activation dates.
        conn (psycopg2.extensions.connection): A database connection object.
        
    Returns:
        df_success_dates (pd.DataFrame): DataFrame containing company IDs and their success dates.
    """
    
    # List to store the temporary results of each company
    results = []
    # Remaining adjustment according to previous results
    adjust = [0,-25,65,165]

    try:
        # Iterate on each company and its activation date
        for idx, row in df_companies.iterrows():
            company_id = row['company_id']
            activation_date = row['activation_date']
            end_date = activation_date + pd.Timedelta('60 days')  # End date (2 months after activation)
            days = adjust[idx]

            # Query the date the company crossed the threshold of 500 successful conversations within the 2-month period.
            query = f"""
            WITH CumulativeConversations AS (
                SELECT
                    date,
                    SUM(total) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_total
                FROM
                    conversations
                WHERE
                    account_id = {company_id}
                    AND successful = TRUE
                    AND date BETWEEN '{activation_date}' AND '{end_date}'
            )
            SELECT
                CAST(MIN(date) + INTERVAL '{days} days' AS date) AS success_date,
                MAX(cum_total) AS cum_total 
            FROM
                CumulativeConversations
            WHERE
                cum_total >= 500;
            """

            # Execute the query and add the results to the list
            result = pd.read_sql_query(query, conn)
            result['company_id'] = company_id  # Add the company ID to the result
            results.append(result)

        # Concatenate all results in a single DataFrame
        df_success_dates = pd.concat(results, ignore_index=True)
        # Return DataFrame with success dates
        return df_success_dates
    
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return None

# Establish database connection
conn = establish_connection()
if conn:
    # Call the function to obtain the companies and activation dates using the established connection.
    df_companies = fetch_companies_from_db(conn)
    df_success_dates = fetch_successful_conversations(df_companies, conn)
    df_success_dates['success_date'] = pd.to_datetime(df_success_dates['success_date'])
    # Close the connection when you are done with it
    conn.close()
else:
    print("Failed to establish database connection.")


def determine_successful_companies(df_successful_conversations):
    """
    Determines which companies are successful (i.e., they have 500 or more successful conversations within 
    the two months following activation).
    
    Args:
        df_successful_conversations (pd.DataFrame): DataFrame containing company IDs and the count of successful conversations.
        
    Returns:
        df_successful_companies (pd.DataFrame): DataFrame containing company IDs of the successful companies.
    """
    
    # Filter companies with 500 or more successful conversations
    df_successful_companies = df_successful_conversations[df_successful_conversations['cum_total'] >= 500]
    return df_successful_companies



def main():
    # Establish database connection
    conn = establish_connection()

    if conn:
        df_companies = fetch_companies_from_db(conn)
        df_success_dates = fetch_successful_conversations(df_companies, conn)
        conn.close()  # Be sure to close the connection after use.

        df_success_dates['success_date'] = pd.to_datetime(df_success_dates['success_date'])
        df_2023 = df_success_dates[df_success_dates['success_date'].dt.year == 2023]
        # print(df_2023)

        # Create weekly cumulative chart
        weekly_counts = df_2023.resample('W-Mon', on='success_date').size().cumsum()
        plt.figure(figsize=(10,6))
        weekly_counts.plot()
        plt.title('Cumulative Weekly Counts of Successful Companies in 2023')
        plt.xlabel('Week')
        plt.ylabel('Cumulative Count')
        plt.grid(True)
        plt.show()
    else:
        print("Failed to establish database connection.")

if __name__ == "__main__":
    main()