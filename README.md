# Welcome!
### __Disclaimer__:
The code in this repository was written by an Analytics Engineer who's more interested in the Data Engineering side of things.  

The python modules in this codebase were developed to aid in the automation and democratization of data in the workplace, with reusability and consistency in mind. The code _will not run as is_ and was written to interact with _specific tools_, which are listed out below. The files in this repo are modules that can be imported into scripts to automate tasks, the movement of data, and to make data available to others. They serve as the foundation for simple scripting.

### Specific Tooling / Use Cases:
- AWS Secrets Manager
    - This codebase will need to run in an environment that has access to AWS Secrets Manager. This was the tool of choice to keep things secure when handle sensitive information like Service Account Keys and Database credentials.

- AWS Redshift
    - Query/load data in an AWS Redshift datawarehouse.

- BigQuery
    - Query/load data in a Google BigQuery datawarehouse.

- MySQL
    - Query data from a MySQL database.

- Google Sheets
    - Import data to google sheets, or read data from google sheets. Also have the ability to delete google sheets.

- Slack
    - Observability is important. Post messages to Slack channels upon job completion.

### Like what you see?
#### If you decide to clone this repo:
If you're a wannabe like me, do yourself a favor and follow these steps to prime your environment, _after_ cloning this repo and changing into the root directory locally:
1. Create a virtual environment to isolate this repos package dependencies: `python3 -m venv your_venv_name`
2. Activate said virtual environment (if on MacOS): `source your_venv_name/bin/activate`
3. Finally, install packages from requirements.txt file: `pip install -r requirements.txt`
