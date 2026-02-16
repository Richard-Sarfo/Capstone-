# Data Quality Validation Framework

A comprehensive, configurable data quality validation solution for enterprise data engineering pipelines. Built to automatically detect, document, and resolve common data quality issues that plague production systems.

---

## 🎯 Problem Statement

Poor data quality is one of the leading causes of failed analytics projects and unreliable business decisions. According to industry research, organizations lose an average of **$12.9 million annually** due to poor data quality.

### Common Issues Addressed
- ❌ **Duplicate Records** - Inflate metrics and create processing bottlenecks
- ❌ **Missing Values** - Break downstream pipelines and skew analyses  
- ❌ **Invalid Data Types** - Cause runtime errors in production systems
- ❌ **Out-of-Range Values** - Indicate data entry errors or corruption
- ❌ **Format Inconsistencies** - Emails, phone numbers, dates with varying patterns
- ❌ **Statistical Outliers** - May indicate fraudulent activity or system glitches

### Solution
An intelligent, modular framework that handles all these issues while providing:
- ✅ Automatic detection and resolution strategies
- ✅ Detailed audit logging and quality reports
- ✅ Seamless ETL/ELT pipeline integration
- ✅ Configurable validation rules
- ✅ Quality metrics for monitoring

---

## 🚀 Features

### Core Validation Methods

| Feature | Description |
|---------|-------------|
| **Duplicate Detection** | Identifies and removes duplicate records based on specified columns |
| **Null Handling** | Multiple strategies: removal, median/mean imputation, forward-fill, backward-fill |
| **Type Validation** | Automatic type casting with error handling and validation |
| **Range Checking** | Validates numeric values are within specified min/max bounds |
| **Pattern Validation** | Regex-based validation for emails, phone numbers, dates, etc. |
| **Outlier Detection** | Statistical outlier identification using IQR and Z-score methods |
| **Required Columns Check** | Ensures mandatory columns exist in the dataset |

### Logging & Reporting

- **Multi-level Logging**: DEBUG, INFO, WARNING, ERROR levels
- **Dual Output**: Console and file-based logging
- **Quality Reports**: JSON-formatted detailed reports with metrics
- **Audit Trail**: Complete history of all validation operations
- **Metrics Tracking**: Rows processed, rows cleaned, issues found

---

## 📋 Requirements

```
pandas>=1.0.0
numpy>=1.18.0
python>=3.7
```

## 💾 Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd Capstone
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

---

## 📂 Project Structure

```
Capstone/
├── README.md                          # This file
├── data_quality_validator.py          # Main validation framework
├── data_quality_documentation.md      # Technical documentation
├── quality_report.json                # Sample output report
├── requirements.txt                   # Python dependencies
├── logs/                              # Validation logs
└── Screenshots/                       # Visual documentation
```

---

## 🔧 Usage

### Basic Example

```python
import pandas as pd
from data_quality_validator import DataQualityValidator

# Load your data
df = pd.read_csv('data.csv')

# Define validation configuration
config = {
    'check_duplicates': {
        'subset': ['user_id'],
        'keep': 'first'
    },
    'required_columns': ['user_id', 'email', 'age'],
    'null_handling': {
        'age': {'strategy': 'fill_median'},
        'email': {'strategy': 'drop_row'}
    },
    'type_validation': {
        'age': 'int',
        'salary': 'float',
        'created_date': 'datetime'
    },
    'range_checks': {
        'age': {'min': 0, 'max': 120},
        'salary': {'min': 0, 'max': 1000000}
    },
    'pattern_validation': {
        'email': {'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'},
        'phone': {'pattern': r'^\d{10}$'}
    },
    'outlier_detection': {
        'salary': {'method': 'iqr', 'threshold': 1.5}
    }
}

# Create validator with logging
validator = DataQualityValidator(
    log_file='logs/validation.log',
    log_level='INFO'
)

# Run validation
cleaned_df, quality_report = validator.validate_and_clean(df, config)

# Save results
cleaned_df.to_csv('cleaned_data.csv', index=False)
print(validator.get_report_json())
```

---

## ⚙️ Configuration Reference

### Configuration Dictionary Structure

```python
config = {
    # Check for duplicate rows
    'check_duplicates': {
        'subset': ['column1', 'column2'],  # Columns to check
        'keep': 'first'                     # 'first', 'last', or False (remove all)
    },
    
    # Required columns that must exist
    'required_columns': ['col1', 'col2'],
    
    # Handle missing values
    'null_handling': {
        'column_name': {
            'strategy': 'fill_median'       # 'drop_row', 'fill_mean', 'fill_median', 'fill_forward', 'fill_backward'
        }
    },
    
    # Validate/convert data types
    'type_validation': {
        'column_name': 'int'               # 'int', 'float', 'string', 'datetime'
    },
    
    # Check numeric ranges
    'range_checks': {
        'column_name': {'min': 0, 'max': 100}
    },
    
    # Pattern matching with regex
    'pattern_validation': {
        'column_name': {'pattern': r'^[A-Z]'}
    },
    
    # Outlier detection
    'outlier_detection': {
        'column_name': {
            'method': 'iqr',                # 'iqr' or 'zscore'
            'threshold': 1.5                 # IQR multiplier or Z-score threshold
        }
    }
}
```

---

## 📊 Output & Reports

### Quality Report JSON

The validator generates a comprehensive JSON report:

```json
{
  "timestamp": "2026-01-27T10:55:44.993039",
  "checks_performed": [
    "duplicate_check",
    "required_columns_check",
    "null_handling",
    "type_validation",
    "range_validation",
    "pattern_validation",
    "outlier_detection"
  ],
  "issues_found": [
    {
      "type": "duplicates",
      "count": 1,
      "columns_checked": ["user_id"]
    },
    {
      "type": "null_values",
      "column": "age",
      "count": 1,
      "percentage": 11.11,
      "strategy": "fill_median"
    }
  ],
  "rows_processed": 9,
  "rows_cleaned": 8
}
```

### Logging Output

Detailed logs capture every validation step:

```
2026-01-27 10:55:44 - DataQualityValidator - INFO - ============================================================
2026-01-27 10:55:44 - DataQualityValidator - INFO - Data Quality Validator Initialized
2026-01-27 10:55:44 - DataQualityValidator - INFO - Starting validation pipeline on 9 rows
2026-01-27 10:55:44 - DataQualityValidator - INFO - Running duplicate check...
2026-01-27 10:55:44 - DataQualityValidator - INFO - Handling null values...
2026-01-27 10:55:44 - DataQualityValidator - INFO - VALIDATION PIPELINE COMPLETE
```

---

## 🔄 Integration with ETL Pipelines

### Apache Airflow Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_quality_validator import DataQualityValidator

def validate_data(**context):
    df = pd.read_csv('raw_data.csv')
    validator = DataQualityValidator(log_file='logs/airflow_validation.log')
    cleaned_df, report = validator.validate_and_clean(df, config)
    cleaned_df.to_parquet('validated_data.parquet')
    return report

with DAG('data_validation_pipeline', ...) as dag:
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )
```

---

## 🛠️ Advanced Features

### Custom Validation Strategies

The framework supports extending validation logic:

```python
class CustomValidator(DataQualityValidator):
    def _custom_validation(self, df, config):
        # Add your custom validation logic
        return df
```

### Batch Processing

```python
validator = DataQualityValidator()

for file in input_files:
    df = pd.read_csv(file)
    cleaned_df, report = validator.validate_and_clean(df, config)
    # Store results
```

---

## 📈 Key Metrics

The validator tracks:
- **Rows Processed**: Total input rows
- **Rows Cleaned**: Output rows after validation
- **Issues Found**: Count and types of quality issues
- **Processing Time**: Validation duration
- **Quality Score**: Percentage of valid records

---

## 🧪 Testing

Run the validator on sample data:

```bash
python -c "
import pandas as pd
from data_quality_validator import DataQualityValidator

# Create sample data
df = pd.DataFrame({
    'user_id': [1, 1, 3],
    'age': [25, 30, 150],
    'email': ['user@example.com', 'invalid-email', 'another@domain.com']
})

config = {
    'check_duplicates': {'subset': ['user_id']},
    'range_checks': {'age': {'min': 0, 'max': 120}},
    'pattern_validation': {'email': {'pattern': r'^[^@]+@[^@]+\.[^@]+$'}}
}

validator = DataQualityValidator()
cleaned_df, report = validator.validate_and_clean(df, config)
print(cleaned_df)
"
```

---

## 📚 Documentation

- **Technical Documentation**: See [data_quality_documentation.md](data_quality_documentation.md) for detailed architecture and design decisions
- **API Reference**: Check docstrings in `data_quality_validator.py`
- **Sample Reports**: View `quality_report.json` for example output format

---

## 🤝 Contributing

Contributions are welcome! Areas for enhancement:

- [ ] Additional outlier detection methods (Isolation Forest, Local Outlier Factor)
- [ ] Distribution-based anomaly detection
- [ ] Support for categorical data validation
- [ ] Machine learning-based data quality scoring
- [ ] Real-time validation streaming
- [ ] Web dashboard for quality metrics

---

## 📝 License

This project is part of the Capstone Portfolio project.

---

## 🙋 Support & Questions

For issues, questions, or feature requests, please refer to the technical documentation or contact the development team.

---

## 🎓 Learning Outcomes

This project demonstrates:
- ✓ Production-grade data validation patterns
- ✓ Logging and observability best practices
- ✓ Configurable framework design patterns
- ✓ Error handling and data resilience
- ✓ ETL/ELT pipeline integration
- ✓ Data quality metrics and monitoring

---

**Last Updated**: February 2026  
**Status**: Production Ready
