# QueryLite

QueryLite is a lightweight Text-to-SQL chatbot built with Streamlit, SQLite, and Groq.
It reads `products.csv`, creates `querylite.db` dynamically, infers schema types, generates SQL from natural language, validates safety rules, and displays results.

## Features 

- Dynamic CSV to SQLite loading at runtime
- Dynamic schema extraction via `sqlite_master` and `PRAGMA table_info`
- Groq-powered natural language to SQL generation
- SQL safety checks (read-only, blocked destructive keywords, enforced `LIMIT`)
- Streamlit UI with generated SQL + tabular results

## Project Structure

- `main.py` - Full Streamlit app and pipeline
- `requirements.txt` - Python dependencies
- `products.csv` - Input data source
- `.env` - Environment variables
- `.quel/` - Existing virtual environment
- `querylite.db` - Auto-created SQLite database

## Environment Variables

Set these in `.env`:

```env
GROQ_API_KEY=your_key_here
GROQ_MODEL=your_model_here
MAX_TOKENS=300
TEMPERATURE=0.1
```

## Setup (Use existing `.quel` environment)

### Windows PowerShell

```powershell
.\.quel\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run main.py
```

### Windows CMD

```cmd
.quel\Scripts\activate.bat
pip install -r requirements.txt
streamlit run main.py
```

## Example Questions

- Show top 5 products by sales
- Which category has highest revenue?
- List best-selling products with rating above 4.5

## Notes

- The app does not hardcode schema/table/column names.
- The SQLite table name is derived from the CSV file name.
- If a query returns no rows, the app shows: `No data found`.
