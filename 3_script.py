import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import os
import warnings
import seaborn as sns
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
    
# Read documentation to understand the function
def fetch_monthly_data(conn, month):
    """
    Fetches invoice data for companies closed in a given month.
    
    Args:
        conn (psycopg2.extensions.connection): A database connection object.
        month (int): The month for which data should be fetched.
        
    Returns:
        df (pd.DataFrame): A DataFrame containing invoice data.
    """
    query = f"""
        WITH CompaniesClosedInMonth AS (
            SELECT id AS company_id
            FROM public.company
            WHERE EXTRACT(YEAR FROM close_date) = 2023 AND EXTRACT(MONTH FROM close_date) = {month}
        ),
        StripeIdsForCompaniesClosedInMonth AS (
            SELECT stripe_company_ids
            FROM public.company_identifiers
            WHERE company_id IN (SELECT company_id FROM CompaniesClosedInMonth)
        ),
        InvoicesForCompaniesClosedInMonth AS (
            SELECT si.company_id, EXTRACT(MONTH FROM si.sent_date) AS invoice_month, SUM(si.amount) AS total_amount
            FROM public.stripe_invoice si
            JOIN StripeIdsForCompaniesClosedInMonth scim ON si.company_id = scim.stripe_company_ids
            WHERE EXTRACT(YEAR FROM si.sent_date) = 2023 AND EXTRACT(MONTH FROM si.sent_date) BETWEEN {month} AND 8
            GROUP BY si.company_id, invoice_month
        )
        SELECT
            '{month}' AS cohort,
            invoice_month,
            SUM(total_amount) AS revenue
        FROM InvoicesForCompaniesClosedInMonth
        GROUP BY invoice_month
        ORDER BY invoice_month;
    """
    df = pd.read_sql(query, conn)
    return df

# Read documentation to understand the function
def generate_mock_data():
    """
    Generates mock invoice data for multiple cohorts.
    
    Returns:
        df (pd.DataFrame): A DataFrame containing mock invoice data.
    """
    data = []

    # Generate data for each cohort
    for month in range(1, 9):
        cohort_data = []
        for i in range(1, 9):
            cohort_data.append([i, get_mock_revenue(month, i)])

        cohort_df = pd.DataFrame(cohort_data, columns=['invoice_month', 'revenue'])
        cohort_df['cohort'] = f'2023-{month:02d}'
        data.append(cohort_df)

    df = pd.concat(data)
    return df

# Read documentation to understand the function
def get_mock_revenue(cohort_month, invoice_month):
    """
    Returns mock revenue data for a specific cohort and invoice month.
    
    Args:
        cohort_month (int): The cohort month.
        invoice_month (int): The invoice month.
        
    Returns:
        revenue (int): Mock revenue amount.
    """
    # Define mock revenue values 
    mock_data = {
        1: [292, 644, 621, 317, 649, 556, 328, 536],
        2: [754, 317, 307, 187, 228, 57, 95],
        3: [51, 693, 173, 99, 356, 378],
        4: [206, 278, 79, 52, 85],
        5: [69, 63, 56, 85],
        6: [404, 160, 329],
        7: [276, 119],
        8: [293]
    }

    cohort_data = mock_data.get(cohort_month, [])
    
    # Check if invoice_month is within valid range
    if 1 <= invoice_month <= len(cohort_data):
        return cohort_data[invoice_month - 1]
    else:
        return   # Return 0 for out-of-range months

def plot_cohort_heatmap(df):
    """
    Plots a cohort heatmap using the provided DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the cohort data.
    """
    pivot = df.pivot_table(index="cohort", columns="invoice_month", values="revenue", fill_value=None)

    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot, annot=True, fmt="g", cmap="YlGnBu", cbar=True, linewidths=0.5, cbar_kws={'label': 'Companies Closed'})
    plt.title("Cohort Analysis - Companies Closed per Month")
    plt.ylabel("Cohort (Close Date)")
    plt.xlabel("Invoice Month (2023)")
    plt.show()
    
def main():
    # Establish database connection
    conn = establish_connection()

    if conn:
         # Initialize a list to store all dataframes
        all_dataframes = []

        # Fetch data for each month from January to August
        for month in range(1, 9):
            monthly_data = fetch_monthly_data(conn, month)
            all_dataframes.append(monthly_data)

        # Concatenate all monthly dataframes
        final_dataframe = pd.concat(all_dataframes)

        mock_data = generate_mock_data()
        plot_cohort_heatmap(mock_data)

        # Close the database connection
        conn.close()
            
    else:   
        print("Failed to establish database connection.")

if __name__ == "__main__":
    main()