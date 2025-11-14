# Talent Match Intelligent  

A data-driven system to identify and analyze the success differentiators among employees based on **competency**, **psychometric**, and **contextual** dimensions.  
This repository integrates SQL logic, Supabase connection, and analytics pipeline to compute **TV–TGV match rates** and build a composite **Success Formula**.

---

## Project Overview
This project provides an end-to-end analytical flow for **talent benchmarking** and **intelligent matching**.  
It enables HR or Talent Development teams to:
- Identify **high-performer benchmarks** (based on top rating per role & year).  
- Evaluate individual employee **match rates** to the benchmark.  
- Combine competency, psychometric, and behavioral indicators into a unified **success score**.

**See the report here: ([Talent Match Intelligent Report](https://www.canva.com/design/DAG2rlHLAjs/cnant7TVtHym_3OiCf9m6w/edit?utm_content=DAG2rlHLAjs&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton))**

**Try talent matching here: ([Talent Match Intelligent](https://talentmatching.streamlit.app/))**

---

## Repository Structure
├── Dataset/ # Folder containing raw and processed data

├── CTE_matching.sql # SQL script for TV–TGV match rate calculation

├── talentBranchmarks.sql # SQL script to generate per-role benchmark

├── ERD.sql # Database schema (Entity Relationship Definition)

├── TalentMatchIntelligent.ipynb # Jupyter Notebook (main analytics & Supabase connection

└── README.md # Project documentation


## Setup Instructions

### 1. Prerequisites
- Python ≥ 3.9  
- Jupyter Notebook or VSCode  
- Supabase account with database access  
- PostgreSQL or pgAdmin (optional for running SQL scripts)

Install the required dependencies:
```bash
pip install supabase psycopg2 pandas numpy sqlalchemy python-dotenv
```
### 2. Environment Setup
Create a .env file in the project root directory and include your credentials:
```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-or-service-key
DATABASE_URL=postgresql://user:password@host:port/database
```

### 3. Running SQL Scripts
- talentBranchmarks.sql
  
  Creates talent_benchmarks table and populates it with top performers per role and year.
  
- CTE_matching.sql
  
  Computes TV (Talent Variable) and TGV (Target Group Variable) match rates for each employee.

## Notebook Overview — TalentMatchIntelligent.ipynb

The notebook automates data extraction and computation by connecting directly to Supabase.

Main Steps:
1. Load environment variables from .env
2. Connect to Supabase and query tables (employees, dim_positions, performance_yearly)
3. Execute the benchmark and match rate SQLs
4. Combine the results into a unified success dataset
5. Visualize competency and psychometric differentiators
