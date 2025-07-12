# Contributing to Gajana

Thank you for your interest in contributing to Gajana! Whether you're fixing a bug, adding a new feature, or improving documentation, your help is greatly appreciated.

## Development Setup

To get started with development, please follow these steps.

### 1. Clone and Set Up Environment

First, clone the repository and set up your Python virtual environment.

```bash
git clone <your-repository-url>
cd gajana
python3 -m venv venv
source venv/bin/activate
```

### 2. Install Dependencies

Install the required dependencies from `requirements.txt`.

```bash
pip install -r requirements.txt
```

### 3. Install Pre-commit Hooks

This project uses `pre-commit` hooks to ensure code quality, formatting, and tests pass before any code is committed. This is a crucial step.

```bash
pre-commit install
```

Now, every time you run `git commit`, the hooks will automatically run linters (`flake8`), formatters (`black`), type checkers (`mypy`), and run the full test suite. If any step fails, the commit will be aborted, allowing you to fix the issue.

---

## How to Contribute

### Adding Support for a New Bank or Credit Card

This is one of the most valuable ways to contribute. The process is designed to be straightforward and requires no changes to the core Python code.

1.  **Add Account Name:**
    Open `src/constants.py` and add your new account identifier to either the `BANK_ACCOUNTS` or `CC_ACCOUNTS` list. The name should be unique and descriptive (e.g., `bank-newbank-savings`).

2.  **Create a New Parser Configuration:**
    -   In the `data/configs/` directory, create a new JSON file. The filename is important and must match the key used by the application. The convention is `{type}-{name}.json` (e.g., `bank-newbank.json`).
    -   This file tells the `TransactionProcessor` how to read the statement. Here is a template:
        ```json
        {
          "header_patterns": [
            ["Date", "Transaction Details", "Withdrawal Amt.", "Deposit Amt."]
          ],
          "column_map": {
            "Date": "date",
            "Transaction Details": "description",
            "Withdrawal Amt.": "debit",
            "Deposit Amt.": "credit"
          },
          "date_formats": ["%d/%m/%y"]
        }
        ```
    -   **`header_patterns`**: A list of possible headers. The processor will use this to find the start of the data.
    -   **`column_map`**: Maps the column names from your statement to the internal names used by the application (`date`, `description`, `debit`, `credit`, `amount`).
    -   **`date_formats`**: A list of possible date formats found in the statement's date column.

3.  **Add a Test Case:**
    To ensure your parser works correctly now and in the future, please add a test for it in the appropriate test file.

### Improving Categorization

-   **Add New Matchers:** To improve automatic categorization, you can add new rule objects to the `data/matchers.json` file.
-   **Use Learn Mode:** Run `python src/main.py --learn-categories` to get suggestions for new rules based on your existing categorized data.

### Bug Fixes and New Features

1.  Create a new branch for your feature or bug fix: `git checkout -b feature/my-new-feature` or `git checkout -b fix/issue-description`.
2.  Write your code.
3.  Add or update tests to cover your changes.
4.  Ensure all pre-commit hooks pass when you commit your changes.

---

## Submitting Changes

1.  Fork the repository.
2.  Create your feature branch from the `main` branch.
3.  Make your changes and commit them with a clear, descriptive message.
4.  Push your branch to your fork.
5.  Open a pull request to the main repository, detailing the changes you've made.

Thank you again for your contribution!
