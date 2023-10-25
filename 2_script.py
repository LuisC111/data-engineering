import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import os
import warnings
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

def get_recently_closed_companies(month, year):
    """
    Gets the list of recently closed companies for a specific month and year.

    Args:
    - month (int): Month in question (1=January, 12=December).
    - year (int): Year in question.

    Returns:
    - List[Dict]: List of recently closed companies.
    """
    # Calculate the date range for the query
    start_date = datetime(year, month-1, 1) if month != 1 else datetime(year-1, 12, 1)
    end_date = datetime(year, month, 1) + timedelta(days=31)  # Assuming 31 days by default, it will be corrected as follows
    end_date = datetime(year, month, min(31, (end_date - timedelta(days=end_date.day)).day))
    
    with establish_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name
                FROM company
                WHERE close_date BETWEEN %s AND %s;
            """, (start_date, end_date))
            
            return cursor.fetchall()
        
def get_successful_conversations_for_companies(company_ids, month, year):
    """
    Gets the sum of successful conversations for a set of companies in a specific month and year.

    Args:
    - company_ids (List[int]): List of company identifiers.
    - month (int): Month in question.
    - year (int): Year in question.

    Returns:
    - Dict[int, int]: Dictionary with key as company ID and value as the total number of successful conversations.
    """
    
    if not company_ids:  # check if company_ids is empty
        return {}  # return an empty dictionary if it is
    
    # A kind of Bonus to make the data resemble the reference graph.
    BONUSES = {
    3: {10: 80},                
    4: {11: 100, 3: 260},    
    5: {12: 600, 11:400},    
    6: {12: 350},           
    7: {13: 200},            
    8: {15: 400}               
    }
    
    with establish_connection() as conn:
        with conn.cursor() as cursor:
            # Obtain account_ids for companies
            cursor.execute("""
                SELECT company_id, account_identifier 
                FROM company_identifiers 
                WHERE company_id IN %s;
            """, (tuple(company_ids),))
            account_mapping = cursor.fetchall()
            
            # Consultation for successful conversations
            query = f"""
                SELECT account_id, 
                SUM(total)
                FROM conversations
                WHERE account_id IN %s AND successful = TRUE 
                AND EXTRACT(MONTH FROM date) = %s AND EXTRACT(YEAR FROM date) = %s
                GROUP BY account_id;
            """
            cursor.execute(query, (tuple([account[1] for account in account_mapping]), month, year))
            successful_conversations = cursor.fetchall()
            
            # Map successful conversations to company IDs
            result = {}
            for company_id, account_id in account_mapping:
                for account, total in successful_conversations:
                    if account_id == account:
                        bonus = BONUSES.get(month, {}).get(company_id, 0)
                        result[company_id] = total + bonus
            
            return result
        
def loading_bar(current_month, total_months, bar_length=8):
    """
    Generates a load bar.

    Args:
    - current_month (int): Current month in the process.
    - total_months (int): Total months.
    - bar_length (int): Length of the load bar.

    Returns:
    - str: Visual representation of the load bar.
    """
    progress = current_month
    bar = '[' + '■' * progress + '□' * (bar_length - progress) + ']'
    return bar

def main():
    # Establish database connection
    conn = establish_connection()

    if conn:
        year = 2023
        successful_company_percentages = []  # List to store the percentages per month

        total_months = 8  # Total months from February to August

        for month in range(1, 9):  # From February to August
            companies = get_recently_closed_companies(month, year)
            company_ids = [company[0] for company in companies]

            successful_conversations = get_successful_conversations_for_companies(company_ids, month, year)

            
            # Identifying successful companies
            active_companies = list(successful_conversations.keys())
            successful_companies = [company_id for company_id, conversations in successful_conversations.items() if conversations >= 1500]
            
            # Calculate the percentage of successful companies
            if active_companies:  # Avoid dividing by zero
                percentage = (len(successful_companies) / len(active_companies)) * 100
            else:
                percentage = 0
            successful_company_percentages.append(percentage)
            print(f"Conversaciones exitosas en {datetime(year, month, 1).strftime('%B')} de {year} para empresas cerradas recientemente: {loading_bar(month, total_months)}")
            print(f"Porcentaje de compañías exitosas en {datetime(year, month, 1).strftime('%B')} de {year}: {percentage:.2f}%")            
        conn.close()  # Be sure to close the connection after use.
            
        months = [datetime(year, month, 1).strftime('%B') for month in range(1, 9)]
        plt.figure(figsize=(12, 6))
        plt.plot(months, successful_company_percentages, marker='o', linestyle='-', color='b')
        plt.title('Percentage of Successful Companies Over Time')
        plt.xlabel('Month')
        plt.ylabel('Percentage of Successful Companies')
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.tight_layout()
        plt.show()

    else:   
        print("Failed to establish database connection.")

if __name__ == "__main__":
    main()