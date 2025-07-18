# Gajana - Personal Finance Automation

Gajana is a powerful, automated data pipeline designed to streamline personal finance management. It intelligently fetches bank and credit card statements from Google Drive, parses them into a standardized format, categorizes transactions based on custom rules, and consolidates everything into a central Google Sheet for easy tracking and analysis.

## Key Features

- **Automated Statement Parsing:** Automatically processes statement files from various banks and credit cards, handling different formats and layouts.
- **Intelligent Categorization:** Applies customizable rules to automatically categorize each transaction, saving hours of manual work.
- **Centralized Data Store:** Consolidates all financial data into a single, clean Google Sheet, acting as your personal finance database.
- **Extensible Architecture:** Easily add support for new bank or credit card statements by adding simple JSON configuration files.
- **Multiple Operating Modes:**
    - **Normal Mode:** Fetches and processes only new transactions since the last run.
    - **Recategorize Mode:** Re-applies categorization rules to all existing transactions.
    - **Learn Mode:** Analyzes your categorized transactions to suggest new rules for uncategorized items.

---

## Data Storage and Format

Gajana relies on a specific structure for storing and processing your financial data using Google Drive and Google Sheets.

### 1. Google Drive Folder for Statements

You will need a dedicated folder in your Google Drive to store your statement files.

-   **Structure:** Create one main folder (e.g., "Bank Statements").
-   **File Type:** Each statement file must be a **Google Sheet**. If you have statements in PDF or CSV format, you must convert them to Google Sheets first.
-   **File Naming Convention:** The application identifies which parser to use based on the filename. The name should follow the pattern `{type}-{name}-{date}.gsheet`, for example:
    -   `bank-hdfc-<name>-2023.gsheet`
    -   `cc-axis-magnus-2024-05.gsheet`

### 2. Master Google Sheet for Transactions

This is the central database where all your processed transactions will be stored.

-   **Create a new Google Sheet.** This will be your master transaction log.
-   **Sheet Tabs:** The workbook must contain two specific tabs (sheets) with the exact names:
    1.  `Bank transactions`
    2.  `CC Transactions`
-   **Column Headers:** Both sheets must have the following headers in the first row, starting from cell A1:
    `Date` | `Description` | `Debit` | `Credit` | `Category` | `Remarks` | `Account`

### 3. Local SQLite Database (for Backup)

For data safety, Gajana uses a local SQLite database as a reliable backup.

-   **Location:** The database file is stored at `DB_FILE_PATH`.
-   **Creation:** This file is created automatically the first time you run the backup command.
-   **Git Ignore:** The `DB_FILE_PATH` file is included in `.gitignore` and should **never** be committed to the repository.

---

## Installation

Follow these steps to set up and run Gajana on your local machine.

### 1. Clone the Repository

```bash
git clone https://github.com/mohankarthik/gajana.git
cd gajana
```

### 2. Create a Virtual Environment

It's highly recommended to use a Python virtual environment to manage dependencies.

```bash
# Create the virtual environment
python3 -m venv venv

# Activate it (on macOS/Linux)
source venv/bin/activate

# Or on Windows
.\venv\Scripts\activate
```

### 3. Install Dependencies

Install all required Python packages using the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

---

## Configuration

Before running the application, you need to configure your Google credentials and set up your data sources.

### 1. Google Service Account Credentials

Gajana uses a Google Cloud Service Account to securely access your Google Drive and Google Sheets.

1.  **Create a Service Account:** Follow the official Google Cloud documentation to [create a service account](https://cloud.google.com/iam/docs/service-accounts-create).
2.  **Enable APIs:** In your Google Cloud project, ensure the **Google Drive API** and **Google Sheets API** are enabled.
3.  **Create a Key:** Generate a JSON key for your service account and download it.
4.  **Place the Key:** Create a `secrets` directory in the project root and place the downloaded key file inside it, renaming it to `google.json`. The final path should be `secrets/google.json`.
5.  **Share Access:**
    * Share the Google Drive folder containing your statements with the service account's email address (e.g., `your-service-account@your-project.iam.gserviceaccount.com`).
    * Share your master Google Sheet with the same service account email address, giving it "Editor" permissions.

### 2. Application Constants (`src/constants.py`)

You need to update a few key constants to point to your specific Google Drive folder and Google Sheet.

-   `CSV_FOLDER`: The ID of the Google Drive folder where you will upload your statement files. To get the ID, open the folder in your browser; the ID is the last part of the URL (e.g., `.../folders/THIS_IS_THE_ID`).
-   `TRANSACTIONS_SHEET_ID`: The ID of your master Google Sheet. You can find this in the URL of your sheet (e.g., `.../d/THIS_IS_THE_ID/edit`).

### 3. Statement Parsing Configurations (`data/configs/`)

This directory contains JSON files that tell the application how to parse different statement formats. Each file corresponds to a specific bank or credit card. You can modify these if your statement format differs.

### 4. Categorization Rules (`data/matchers.json`)

This file contains the rules used to categorize your transactions. You can add or edit rules here to customize how your expenses and income are classified.

---

## Running the Application

Gajana is run from the command line from the root of the project directory.

### Normal Mode

This is the default mode. It fetches new statements, processes new transactions, and appends them to your Google Sheet.

```bash
python main.py
```

### Recategorize Mode

This mode re-applies the rules in `data/matchers.json` to all existing transactions in your Google Sheet. This is useful after you've updated your categorization rules.

```bash
python main.py --recategorize-only
```

### Learn Mode

This mode analyzes your already-categorized transactions to find common patterns and suggests new rules that you can add to `data/matchers.json` to reduce uncategorized items.

```bash
python main.py --learn-categories
```

### Backup and Restore Mode

These commands allow you to sync data between your primary Google Sheet and your local SQLite database backup.

**Backup from Google Sheets to Local DB:**
This command reads all data from your Google Sheets and saves it to the local `DB_FILE_PATH` file. This is the recommended way to create a safe, local backup.

```bash
python main.py --backup-db
```

**Restore from Local DB to Google Sheets (DESTRUCTIVE):**
This command will completely erase the data in your Google Sheets and replace it with the data from your local `DB_FILE_PATH` file. Use this with caution.

```bash
python main.py --restore-db
