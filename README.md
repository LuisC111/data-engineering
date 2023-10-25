# Data engineering
Data engineering and analysis test scripts with python and postgresql

In order to have the best programming practices I implemented a connection handling which is important to make sure that the connection to the database is closed properly, even if an error occurs somewhere in the process. A with block is used to handle the connection and ensure that it is properly closed after use.

For security reasons sending passwords in the code is not a good practice. So the credentials are stored securely in the .env file.

## How does the .env file work?
You must rename the .env.example file to .env and change the value of the variables to the actual connection to the database. (The .env file with the values I sent to your email along with the link to the repository).

## Requirements:

There are two ways to install the libraries needed to run the scripts.

The first is to open the terminal and enter the following command:
pip install -r requirements.txt

The second is to execute the following commands one by one:

pip install psycopg2-binary
pip install pandas
pip install python-dotenv
pip install matplotlib
pip install seaborn

## How to run the scripts? Open the terminal and run the following commands one by one to see the result of each script.
python .\1_script.py
python .\2_script.py
python .\3_script.py

In case of doubts or questions, please consult the repository documentation ("docs.pdf") or contact me via email or whatsapp.
