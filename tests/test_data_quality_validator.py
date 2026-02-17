import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path to import DataQualityValidator
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_quality_validator import DataQualityValidator

@pytest.fixture
def validator():
    return DataQualityValidator(log_level='DEBUG')

def test_duplicates_remove():
    # Scenario: test_duplicates_remove
    # Input: {'id': [1, 1, 2, 3], 'val': ['a', 'a', 'b', 'c']}
    df = pd.DataFrame({'id': [1, 1, 2, 3], 'val': ['a', 'a', 'b', 'c']})
    
    config = {
        "check_duplicates": {
          "subset": ["id"],
          "keep": "first",
          "action": "remove"
        }
    }
    
    validator = DataQualityValidator()
    cleaned_df, report = validator.validate_and_clean(df, config)
    
    # Expected Rows: 3
    assert len(cleaned_df) == 3
    # Expected Issues: 1 duplicate
    assert len(report['issues_found']) == 1
    assert report['issues_found'][0]['type'] == 'duplicates'

def test_null_handling_median():
    # Scenario: test_null_handling_median
    # Input: {'age': [20, 30, None, 40, 100]}
    df = pd.DataFrame({'age': [20, 30, None, 40, 100]})
    # Converting None to NaN for pandas consistency if needed, though pd.DataFrame handles None
    
    config = {
        "null_handling": {
          "age": "fill_median"
        }
    }
    
    validator = DataQualityValidator()
    cleaned_df, report = validator.validate_and_clean(df, config)
    
    # Expected Rows: 5 (none dropped)
    assert len(cleaned_df) == 5
    # Expected Issues: 1 null_value
    assert len(report['issues_found']) == 1
    assert report['issues_found'][0]['type'] == 'null_values'
    # Check median filling (median of 20, 30, 40, 100 is 35.0)
    assert cleaned_df['age'].isnull().sum() == 0
    # 20, 30, 40, 100 -> sorted: 20, 30, 40, 100. Median is (30+40)/2 = 35
    assert cleaned_df['age'].iloc[2] == 35.0

def test_range_check_remove():
    # Scenario: test_range_check_remove
    # Input: {'score': [50, 150, -10, 80]}
    df = pd.DataFrame({'score': [50, 150, -10, 80]})
    
    config = {
        "range_checks": {
          "score": {"min": 0, "max": 100, "action": "remove"}
        }
    }
    
    validator = DataQualityValidator()
    cleaned_df, report = validator.validate_and_clean(df, config)
    
    # Expected Rows: 2 (50 and 80 remain)
    assert len(cleaned_df) == 2
    # Expected Issues: 2 range_violations (one low, one high)
    # Note: The validator might report them as separate issues or grouped. 
    # Based on code reading: it checks min (1 issue found), then max (1 issue found).
    # so we expect 2 issues in the list.
    assert len(report['issues_found']) == 2
    types = [i['type'] for i in report['issues_found']]
    assert all(t == 'range_violation' for t in types)

def test_type_validation_coercion():
    # Scenario: test_type_validation_coercion
    # Input: {'numeric_str': ['1', '2', '3.5', 'invalid']}
    # Note: 'invalid' will coerce to NaN for float
    df = pd.DataFrame({'numeric_str': ['1', '2', '3.5', 'invalid']})
    
    config = {
        "type_validation": {
          "numeric_str": "float"
        }
    }
    
    validator = DataQualityValidator()
    cleaned_df, report = validator.validate_and_clean(df, config)
    
    # Expected Rows: 4 (coercion doesn't drop rows by default, just makes NaN)
    assert len(cleaned_df) == 4
    # Expected Issues: 0 (The code catches exceptions but pd.to_numeric with coerce just produces NaNs, it doesn't raise exception unless we check for it)
    # Wait, looking at validator code: 
    # `df[col] = pd.to_numeric(df[col], errors='coerce')`
    # It logs success. It doesn't seem to flag 'invalid' as an issue if it becomes NaN, unless Type Validation explicitly checks for NaNs after?
    # The code `try...except` wraps the conversion. `errors='coerce'` suppresses exception.
    # So `issues_found` should be 0 based on current implementation.
    assert len(report['issues_found']) == 0
    assert pd.isna(cleaned_df['numeric_str'].iloc[3])

def test_pattern_validation_email():
    # Scenario: test_pattern_validation_email
    # Input: {'email': ['valid@test.com', 'invalid-email', 'foo@bar']}
    df = pd.DataFrame({'email': ['valid@test.com', 'invalid-email', 'foo@bar']})
    
    config = {
        "pattern_validation": {
          "email": {"pattern": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", "action": "flag"}
        }
    }
    
    validator = DataQualityValidator()
    cleaned_df, report = validator.validate_and_clean(df, config)
    
    # Expected Rows: 3 (flagged, not removed)
    assert len(cleaned_df) == 3
    # Expected Issues: 1 (The code groups invalid counts per column)
    # "Column 'email': 2 values don't match pattern" -> One issue object with count=2
    assert len(report['issues_found']) == 1
    issue = report['issues_found'][0]
    assert issue['type'] == 'pattern_violation'
    assert issue['count'] == 2
    assert 'email_invalid_format' in cleaned_df.columns

def test_outlier_detection_iqr():
    # Scenario: test_outlier_detection_iqr
    # Input: {'salary': [50000, 52000, 51000, 49000, 1000000]}
    df = pd.DataFrame({'salary': [50000.0, 52000.0, 51000.0, 49000.0, 1000000.0]})
    
    config = {
        "outlier_detection": {
          "salary": {"method": "iqr", "action": "remove"}
        }
    }
    
    validator = DataQualityValidator()
    cleaned_df, report = validator.validate_and_clean(df, config)
    
    # Expected Rows: 4 (1 million removed)
    assert len(cleaned_df) == 4
    # Expected Issues: 1
    assert len(report['issues_found']) == 1
    assert report['issues_found'][0]['type'] == 'outliers'
