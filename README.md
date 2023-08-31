# sparql-log-analysis
Project to extract and analyze SPARQL log files

## Install

Create and activate virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies
```bash
pip install -e .
```


With hatch
```bash
hatch run python src/analyze_sparql_logs.py
```

Without hatch
```bash
python src/analyze_sparql_logs.py
```
