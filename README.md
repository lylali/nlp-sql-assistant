# 🧠 Legacy Database Assistant

An interactive **Natural Language → SQL** system designed to help developers and analysts query complex **legacy-style databases** without manual SQL knowledge.

This project was developed as part of a postgraduate dissertation, demonstrating how **AI-assisted query generation**, **schema learning**, and **user feedback loops** can enhance data accessibility in long-lived enterprise databases.


## 🎯 Project Overview

The Legacy Database Assistant provides:

- **Synthetic Legacy Database** (Atradius-inspired) — reproducible schema with realistic entities: policies, claims, users, organizations, etc.
- **Question-to-SQL Generation Pipeline**
  - Hybrid **rule-based** + **retrieval-based** NLP pipeline.
  - Dynamic schema learning to extract tables, columns, and example values.
- **Feedback Loop**
  - User corrections stored and reintegrated into training corpus.
  - Scores and ranking dynamically improve from feedback.
- **Interactive Streamlit Interface**
  - Ask natural language questions.
  - See generated SQL.
  - Inspect results, correct mistakes, and give feedback.



## 🏗️ System Architecture
Synthetic DB (SQLite)
│
Schema Introspection ──► Template Library ──► Retriever
│ │
▼ ▼
Rule-based NL→SQL Engine ◄──── Feedback Loop
│
▼
Interactive UI (Streamlit)
```yaml
```

## 🚀 Quick Start (Local)

### 1️⃣ Clone the repository
```bash
git clone https://github.com/<your-username>/legacy-database-assistant.git
cd legacy-database-assistant
```
### 2️⃣ Set up environment
```bash
python -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```
### 3️⃣ Run the Streamlit app
```bash
streamlit run apps/streamlit_app.py
```
Then open the link shown in the terminal (usually http://localhost:8501
).

### 4️⃣ Command-Line Interface (optional)
You can also query the assistant directly from the CLI:
```bash
python cli.py -q "how many policies are active"
python cli.py -q "unique status in claims"
python cli.py -q "show organizations where city = london"
```

## 🧩 Example Questions
Example Question
How many policies are active right now?	
Unique status in claims	
Top 10 organizations by credit limit	
Show claims for policy POL-0003	

## 🧠 Features & Learning
- **Schema-Aware Querying** — automatically maps natural language tokens to table and column names.
- **Adaptive Learning** — feedback corrections are merged into a growing template corpus.
**Explainability** — every query includes a rationale for its generation.
- **Offline & Reproducible** — no external APIs or ML dependencies; uses pure Python + SQLite.

## 🧩 Folder Structure
```csharp
legacy-database-assistant/
├── apps/
│   └── streamlit_app.py        # Streamlit front-end
├── legacy_assistant/
│   ├── db.py                   # SQLite demo database
│   ├── nl2sql.py               # Main NL→SQL logic
│   ├── learner.py              # Schema learner
│   ├── predictor.py            # Table/column predictor
│   ├── feedback_learn.py       # Feedback ingestion
│   ├── dynamic_templates.py    # Template auto-generator
│   ├── retriever.py            # TF–IDF retriever
│   └── templates.py            # Static examples
├── cli.py                      # Command-line interface
├── requirements.txt
└── README.md
```

##📄 License
This project is distributed under the MIT License.
Feel free to fork, reuse, or extend with attribution.