import yfinance as yf
import pandas as pd
import pyodbc
import os
import json
from google import genai
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())
#Establish connection to SQL DB
DRIVER_NAME = os.getenv("SQL_DRIVER")  
SERVER_NAME = os.getenv("SQL_SERVER")  
DATABASE_NAME = os.getenv("SQL_DATABASE")  

connection_string = (
    f"DRIVER={{{DRIVER_NAME}}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    "Trusted_Connection=yes;"
)

pyodbc.connect(connection_string)

#Creating SQL Engine
engine = create_engine(
    f"mssql+pyodbc://@{SERVER_NAME}/{DATABASE_NAME}"
    f"?driver={DRIVER_NAME.replace(' ', '+')}",
    fast_executemany=True
)

#API Key
api_key = os.getenv("GEMINI_API_KEY")

#User Input stock ticker
ticker = input("Enter stock ticker:").upper()

stock = yf.Ticker(ticker)

#Clear out previous data in the DB
with engine.begin() as conn:
    conn.execute(text("DELETE FROM fact_prices"))
    conn.execute(text("DELETE FROM fact_financials"))
    conn.execute(text("DELETE FROM fact_valuation_snapshot"))
    conn.execute(text("DELETE FROM dim_date"))
    conn.execute(text("DELETE FROM dim_company"))

#Calling Gemini for PEG and Sector Average PE's
client = genai.Client(api_key=api_key)
prompt = f"Find real-time financial metrics for {ticker} in the stock's sector: 1. Sector avg P/E, 2. {ticker} PEG Ratio. Return ONLY a JSON object, NO intro, NO conversation, ONLY JSON: {{\"sector_avg_pe\": value, \"peg_ratio\": value}}"
response = client.models.generate_content(
    model="gemini-2.0-flash-lite",
    contents=prompt,
    config={'tools': [{'google_search': {}}]} # This ensures the data is 2025-current
)

import json
clean_text = response.text.replace('```json', '').replace('```', '').strip()
ai_data = json.loads(clean_text)

sector_pe = ai_data.get("sector_avg_pe")
peg_ratio = ai_data.get("peg_ratio")


#Extracting and inserting info into dim_company in SQL
info = stock.info

company_df = pd.DataFrame([{
    "ticker": ticker,
    "company_name": info.get("longName"),
    "ceo_name": info.get("companyOfficers")[0].get("name") if info.get("companyOfficers") else None,  
    "sector": info.get("sector"),
    "industry": info.get("industry"),
    "description": info.get("longBusinessSummary"),
    "hq_location": f"{info.get('city', 'Unknown')}, {info.get('state', 'Unknown')}, {info.get('country', 'Unknown')}",
    "employee_count": info.get("fullTimeEmployees")
}])

company_df.to_sql(
    "dim_company",
    engine,
    if_exists="append",
    index=False
)

#Extract fact_prices from yf (not inserting it into SQL yet)
prices = stock.history(period="max")

prices = prices.reset_index()

prices["Date"] = pd.to_datetime(prices["Date"], utc=True) 

prices = prices.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]

prices.columns = ["date", "open_price", "highest_price", "lowest_price", "close_price", "volume"]

prices["date"] = prices["date"].dt.date

prices["ticker"] = ticker

prices = prices[["ticker", "date", "open_price", "highest_price", "lowest_price", "close_price", "volume"]]

#Build out dim_date off of fact_prices data and insert it into SQL
dates = prices[["date"]].drop_duplicates()

today_obj = pd.Timestamp.today().date()
if today_obj not in dates['date'].values:
    dates = pd.concat([dates, pd.DataFrame({'date': [today_obj]})], ignore_index=True)

dates["date"] = pd.to_datetime(dates["date"])

dates["year"] = dates["date"].dt.year
dates["quarter"] = "Q" + dates["date"].dt.quarter.astype(str)
dates["month"] = dates["date"].dt.month
dates["month_name"] = dates["date"].dt.month_name()

dates.to_sql(
    "dim_date",
    engine,
    if_exists="append",
    index=False
)

#Insert fact_prices to sql
prices.to_sql(
    "fact_prices",
    engine,
    if_exists="append",
    index=False,
    chunksize=500
)

#Gathering fact_valuation_snapshot info
valuation_snapshot = pd.DataFrame([{
    "ticker": ticker,
    "snapshot_date": pd.Timestamp.today().date(),
    "market_cap": info.get("marketCap"),
    "pe_ratio": info.get("trailingPE"),
    "sector_pe_ratio": sector_pe,
    "enterprise_value": info.get("enterpriseValue"),
    "dividend_yield": (info.get("dividendYield", 0.0) / 100) if info.get("dividendYield") else 0.0,
    "peg_ratio": peg_ratio

}])

valuation_snapshot.rename(columns={"snapshot_date": "date"}, inplace=True)


#Creating fact_financials from income cashflow and balance sheets
income = stock.quarterly_income_stmt.T.reset_index().rename(columns={"index": "fiscal_date"})
cashflow = stock.quarterly_cashflow.T.reset_index().rename(columns={"index": "fiscal_date"})
balancesheet = stock.quarterly_balance_sheet.T.reset_index().rename(columns={"index": "fiscal_date"})

inc_map = {
    "Total Revenue": "revenue",
    "Net Income": "net_income",
    "EBITDA": "ebitda",
    "Diluted EPS": "eps"
    }
cf_map = {
    "Free Cash Flow": "free_cashflow"
    }
bs_map = {
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Stockholders Equity": "total_equity",
    "Total Stockholder Equity": "total_equity" 
}

income.rename(columns=inc_map, inplace=True)
cashflow.rename(columns=cf_map, inplace=True)
balancesheet.rename(columns=bs_map, inplace=True)

if 'free_cashflow' not in cashflow.columns:
    cashflow['free_cashflow'] = None

#consolidating different statement info into one df
df_final = pd.merge(income, cashflow[['fiscal_date', 'free_cashflow']], on='fiscal_date', how='left')
df_final = pd.merge(df_final, balancesheet[['fiscal_date', 'total_liabilities', 'total_equity']], on='fiscal_date', how='left')

info_debt = stock.info.get('totalDebt', 0)
df_final["total_liabilities"] = info_debt

required_cols = ["revenue", "net_income", "ebitda", "eps", "free_cashflow", "total_liabilities", "total_equity"]
for col in required_cols:
    if col not in df_final.columns:
        df_final[col] = None

#fiscal date within fact_financials
df_final["ticker"] = ticker
df_final["fiscal_date"] = pd.to_datetime(df_final["fiscal_date"])
df_final["fiscal_quarter"] = (
    df_final["fiscal_date"].dt.year.astype(str) + 
    "Q" + 
    df_final["fiscal_date"].dt.quarter.astype(str)
)

#Sending fact_financials to SQL
financials_final = df_final[[
    "ticker",
    "fiscal_date",
    "fiscal_quarter", 
    "revenue",
    "net_income",
    "ebitda",
    "eps", 
    "free_cashflow",
    "total_liabilities",
    "total_equity"
]]

financials_final.to_sql(
    "fact_financials",
    engine,
    if_exists="append",
    index=False,
    chunksize=25
)

##-----Warren Buffet AI lol

#Giving existing financial info for Warren to judge
financials_text = financials_final.to_string()
snapshot_text = valuation_snapshot.to_string()

prompt = f"""
Act as a strictly conservative Warren Buffet style financial analyst. 
Analyze the following data for {ticker}.

CRITERIA:
1. Valuation (P/E, EV/EBITDA vs Historicals)
2. Stability (Free Cash Flow growth, Net Income consistency)
3. Leverage (Debt/Equity, Interest Coverage)
4. Dividend Sustainability (if applicable)

FINANCIAL STATEMENTS:
{financials_text}

CURRENT PRICE SNAPSHOT:
{snapshot_text}

OUTPUT FORMAT:
Return a JSON object exactly like this:
{{
    "ticker": "{ticker}",
    "buffet_score": 1-10,
    "reasoning": "1-2 sentence summary of why this score was given. Keep it to 42 words MAX",
    "risk_factor": "Primary concern."
}}
"""

Warren_AI_Score = client.models.generate_content(
    model="gemini-2.0-flash-lite", 
    contents=prompt
)

clean_score_text = Warren_AI_Score.text.replace('```json', '').replace('```', '').strip()
score_data = json.loads(clean_score_text)

#Adding new columns to fact_valuation_snapshot
valuation_snapshot["buffet_score"] = score_data.get("buffet_score")
valuation_snapshot["ai_reasoning"] = score_data.get("reasoning")
valuation_snapshot["risk_factor"] = score_data.get("risk_factor")

#THEN inserting fact_valuation_snapshot because Warren AI
valuation_snapshot.to_sql(
    "fact_valuation_snapshot",
    engine,
    if_exists="append",
    index=False
)
