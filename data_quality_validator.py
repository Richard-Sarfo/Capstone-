import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Any
import json
import logging
from pathlib import Path

class DataQualityValidator:
    """
    A comprehensive data quality validation framework for data engineering pipelines.
    Handles common data quality issues: nulls, duplicates, format inconsistencies, outliers.
    Includes detailed logging capabilities.
    """
    
    def __init__(self, log_file: str = None, log_level: str = 'INFO'):
        """
        Initialize the validator with logging configuration.
        
        Args:
            log_file: Path to log file. If None, logs to console only.
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.quality_report = {
            'timestamp': datetime.now().isoformat(),
            'checks_performed': [],
            'issues_found': [],
            'rows_processed': 0,
            'rows_cleaned': 0
        }
        
        # Setup logging
        self._setup_logging(log_file, log_level)
        self.logger.info("="*60)
        self.logger.info("Data Quality Validator Initialized")
        self.logger.info(f"Session started at: {self.quality_report['timestamp']}")
        self.logger.info("="*60)
    
    def _setup_logging(self, log_file: str, log_level: str):
        """Configure logging with both file and console handlers."""
        self.logger = logging.getLogger('DataQualityValidator')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (if log_file specified)
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def validate_and_clean(self, df: pd.DataFrame, config: Dict) -> Tuple[pd.DataFrame, Dict]:
        """
        Main method to validate and clean data based on configuration.
        
        Args:
            df: Input DataFrame
            config: Validation configuration dictionary
            
        Returns:
            Tuple of (cleaned_df, quality_report)
        """
        self.quality_report['rows_processed'] = len(df)
        self.logger.info(f"Starting validation pipeline on {len(df)} rows")
        self.logger.info(f"DataFrame shape: {df.shape}")
        self.logger.info(f"Columns: {list(df.columns)}")
        
        cleaned_df = df.copy()
        
        # Check for duplicates
        if config.get('check_duplicates'):
            self.logger.info("Running duplicate check...")
            cleaned_df = self._handle_duplicates(
                cleaned_df, 
                config['check_duplicates']
            )
        
        # Validate required columns
        if config.get('required_columns'):
            self.logger.info("Validating required columns...")
            self._validate_required_columns(
                cleaned_df, 
                config['required_columns']
            )
        
        # Handle missing values
        if config.get('null_handling'):
            self.logger.info("Handling null values...")
            cleaned_df = self._handle_nulls(
                cleaned_df, 
                config['null_handling']
            )
        
        # Validate data types
        if config.get('type_validation'):
            self.logger.info("Validating data types...")
            cleaned_df = self._validate_types(
                cleaned_df, 
                config['type_validation']
            )
        
        # Check value ranges
        if config.get('range_checks'):
            self.logger.info("Checking value ranges...")
            cleaned_df = self._validate_ranges(
                cleaned_df, 
                config['range_checks']
            )
        
        # Pattern validation (regex)
        if config.get('pattern_validation'):
            self.logger.info("Validating patterns...")
            cleaned_df = self._validate_patterns(
                cleaned_df, 
                config['pattern_validation']
            )
        
        # Outlier detection
        if config.get('outlier_detection'):
            self.logger.info("Detecting outliers...")
            cleaned_df = self._detect_outliers(
                cleaned_df, 
                config['outlier_detection']
            )
        
        self.quality_report['rows_cleaned'] = len(cleaned_df)
        rows_removed = self.quality_report['rows_processed'] - self.quality_report['rows_cleaned']
        
        self.logger.info("="*60)
        self.logger.info("VALIDATION PIPELINE COMPLETE")
        self.logger.info(f"Rows processed: {self.quality_report['rows_processed']}")
        self.logger.info(f"Rows in cleaned dataset: {self.quality_report['rows_cleaned']}")
        self.logger.info(f"Rows removed: {rows_removed}")
        self.logger.info(f"Data quality issues found: {len(self.quality_report['issues_found'])}")
        self.logger.info("="*60)
        
        return cleaned_df, self.quality_report
    
    def _handle_duplicates(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Remove or flag duplicate rows."""
        subset = config.get('subset')
        keep = config.get('keep', 'first')
        
        self.logger.debug(f"Checking duplicates on columns: {subset}")
        
        initial_count = len(df)
        duplicates = df.duplicated(subset=subset, keep=keep)
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            self.logger.warning(f"Found {duplicate_count} duplicate rows")
            
            issue = {
                'type': 'duplicates',
                'count': int(duplicate_count),
                'columns_checked': subset
            }
            self.quality_report['issues_found'].append(issue)
            
            if config.get('action') == 'remove':
                df = df[~duplicates]
                self.logger.info(f"Removed {duplicate_count} duplicate rows")
            elif config.get('action') == 'flag':
                df['_is_duplicate'] = duplicates
                self.logger.info(f"Flagged {duplicate_count} duplicate rows")
        else:
            self.logger.info("No duplicates found")
        
        self.quality_report['checks_performed'].append('duplicate_check')
        return df
    
    def _validate_required_columns(self, df: pd.DataFrame, required_cols: List[str]):
        """Ensure required columns exist."""
        self.logger.debug(f"Checking for required columns: {required_cols}")
        
        missing_cols = set(required_cols) - set(df.columns)
        
        if missing_cols:
            self.logger.error(f"Missing required columns: {missing_cols}")
            issue = {
                'type': 'missing_columns',
                'columns': list(missing_cols)
            }
            self.quality_report['issues_found'].append(issue)
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        self.logger.info(f"All required columns present")
        self.quality_report['checks_performed'].append('required_columns_check')
    
    def _handle_nulls(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Handle missing/null values according to strategy."""
        for col, strategy in config.items():
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found in DataFrame, skipping null handling")
                continue
            
            null_count = df[col].isnull().sum()
            
            if null_count > 0:
                null_pct = (null_count / len(df)) * 100
                self.logger.warning(
                    f"Column '{col}': {null_count} null values ({null_pct:.2f}%) - "
                    f"Strategy: {strategy}"
                )
                
                issue = {
                    'type': 'null_values',
                    'column': col,
                    'count': int(null_count),
                    'percentage': round(null_pct, 2),
                    'strategy': strategy
                }
                self.quality_report['issues_found'].append(issue)
                
                if strategy == 'drop_rows':
                    rows_before = len(df)
                    df = df.dropna(subset=[col])
                    rows_dropped = rows_before - len(df)
                    self.logger.info(f"Dropped {rows_dropped} rows with null values in '{col}'")
                    
                elif strategy == 'fill_mean':
                    mean_val = df[col].mean()
                    df[col].fillna(mean_val, inplace=True)
                    self.logger.info(f"Filled nulls in '{col}' with mean: {mean_val:.2f}")
                    
                elif strategy == 'fill_median':
                    median_val = df[col].median()
                    df[col].fillna(median_val, inplace=True)
                    self.logger.info(f"Filled nulls in '{col}' with median: {median_val:.2f}")
                    
                elif strategy == 'fill_mode':
                    mode_val = df[col].mode()[0]
                    df[col].fillna(mode_val, inplace=True)
                    self.logger.info(f"Filled nulls in '{col}' with mode: {mode_val}")
                    
                elif isinstance(strategy, dict) and 'fill_value' in strategy:
                    fill_val = strategy['fill_value']
                    df[col].fillna(fill_val, inplace=True)
                    self.logger.info(f"Filled nulls in '{col}' with custom value: {fill_val}")
            else:
                self.logger.debug(f"Column '{col}': No null values")
        
        self.quality_report['checks_performed'].append('null_handling')
        return df
    
    def _validate_types(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Validate and convert data types."""
        for col, expected_type in config.items():
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found, skipping type validation")
                continue
            
            self.logger.debug(f"Converting '{col}' to type '{expected_type}'")
            
            try:
                if expected_type == 'int':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif expected_type == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif expected_type == 'string':
                    df[col] = df[col].astype(str)
                elif expected_type == 'datetime':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif expected_type == 'bool':
                    df[col] = df[col].astype(bool)
                
                self.logger.info(f"Successfully converted '{col}' to {expected_type}")
                    
            except Exception as e:
                self.logger.error(f"Type conversion failed for '{col}': {str(e)}")
                issue = {
                    'type': 'type_conversion_error',
                    'column': col,
                    'expected_type': expected_type,
                    'error': str(e)
                }
                self.quality_report['issues_found'].append(issue)
        
        self.quality_report['checks_performed'].append('type_validation')
        return df
    
    def _validate_ranges(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Check if values fall within acceptable ranges."""
        for col, range_config in config.items():
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found, skipping range check")
                continue
            
            min_val = range_config.get('min')
            max_val = range_config.get('max')
            action = range_config.get('action', 'flag')
            
            self.logger.debug(f"Checking range for '{col}': min={min_val}, max={max_val}")
            
            if min_val is not None:
                out_of_range = df[col] < min_val
                violation_count = out_of_range.sum()
                
                if violation_count > 0:
                    self.logger.warning(
                        f"Column '{col}': {violation_count} values below minimum ({min_val})"
                    )
                    
                    issue = {
                        'type': 'range_violation',
                        'column': col,
                        'violation': 'below_minimum',
                        'count': int(violation_count),
                        'threshold': min_val
                    }
                    self.quality_report['issues_found'].append(issue)
                    
                    if action == 'remove':
                        df = df[~out_of_range]
                        self.logger.info(f"Removed {violation_count} rows below minimum")
                    elif action == 'cap':
                        df.loc[out_of_range, col] = min_val
                        self.logger.info(f"Capped {violation_count} values to minimum")
            
            if max_val is not None:
                out_of_range = df[col] > max_val
                violation_count = out_of_range.sum()
                
                if violation_count > 0:
                    self.logger.warning(
                        f"Column '{col}': {violation_count} values above maximum ({max_val})"
                    )
                    
                    issue = {
                        'type': 'range_violation',
                        'column': col,
                        'violation': 'above_maximum',
                        'count': int(violation_count),
                        'threshold': max_val
                    }
                    self.quality_report['issues_found'].append(issue)
                    
                    if action == 'remove':
                        df = df[~out_of_range]
                        self.logger.info(f"Removed {violation_count} rows above maximum")
                    elif action == 'cap':
                        df.loc[out_of_range, col] = max_val
                        self.logger.info(f"Capped {violation_count} values to maximum")
        
        self.quality_report['checks_performed'].append('range_validation')
        return df
    
    def _validate_patterns(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Validate string patterns using regex."""
        for col, pattern_config in config.items():
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found, skipping pattern validation")
                continue
            
            pattern = pattern_config['pattern']
            action = pattern_config.get('action', 'flag')
            
            self.logger.debug(f"Validating pattern for '{col}': {pattern}")
            
            # Check which values don't match pattern
            invalid = ~df[col].astype(str).str.match(pattern, na=False)
            invalid_count = invalid.sum()
            
            if invalid_count > 0:
                self.logger.warning(
                    f"Column '{col}': {invalid_count} values don't match pattern"
                )
                
                # Log some examples of invalid values
                invalid_examples = df.loc[invalid, col].head(3).tolist()
                self.logger.debug(f"Invalid pattern examples: {invalid_examples}")
                
                issue = {
                    'type': 'pattern_violation',
                    'column': col,
                    'count': int(invalid_count),
                    'pattern': pattern
                }
                self.quality_report['issues_found'].append(issue)
                
                if action == 'remove':
                    df = df[~invalid]
                    self.logger.info(f"Removed {invalid_count} rows with invalid format")
                elif action == 'flag':
                    df[f'{col}_invalid_format'] = invalid
                    self.logger.info(f"Flagged {invalid_count} rows with invalid format")
            else:
                self.logger.info(f"All values in '{col}' match pattern")
        
        self.quality_report['checks_performed'].append('pattern_validation')
        return df
    
    def _detect_outliers(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Detect outliers using IQR or Z-score method."""
        for col, outlier_config in config.items():
            if col not in df.columns or df[col].dtype not in [np.float64, np.int64]:
                self.logger.warning(f"Column '{col}' not suitable for outlier detection, skipping")
                continue
            
            method = outlier_config.get('method', 'iqr')
            action = outlier_config.get('action', 'flag')
            
            self.logger.debug(f"Detecting outliers in '{col}' using {method} method")
            
            if method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                multiplier = outlier_config.get('multiplier', 1.5)
                
                lower_bound = Q1 - multiplier * IQR
                upper_bound = Q3 + multiplier * IQR
                
                outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
                self.logger.debug(
                    f"IQR bounds: [{lower_bound:.2f}, {upper_bound:.2f}]"
                )
                
            elif method == 'zscore':
                threshold = outlier_config.get('threshold', 3)
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                outliers = z_scores > threshold
                self.logger.debug(f"Z-score threshold: {threshold}")
            
            outlier_count = outliers.sum()
            
            if outlier_count > 0:
                outlier_pct = (outlier_count / len(df)) * 100
                self.logger.warning(
                    f"Column '{col}': {outlier_count} outliers detected ({outlier_pct:.2f}%)"
                )
                
                # Log some outlier examples
                outlier_examples = df.loc[outliers, col].head(3).tolist()
                self.logger.debug(f"Outlier examples: {outlier_examples}")
                
                issue = {
                    'type': 'outliers',
                    'column': col,
                    'count': int(outlier_count),
                    'percentage': round(outlier_pct, 2),
                    'method': method
                }
                self.quality_report['issues_found'].append(issue)
                
                if action == 'remove':
                    df = df[~outliers]
                    self.logger.info(f"Removed {outlier_count} outlier rows")
                elif action == 'flag':
                    df[f'{col}_is_outlier'] = outliers
                    self.logger.info(f"Flagged {outlier_count} outlier rows")
            else:
                self.logger.info(f"No outliers detected in '{col}'")
        
        self.quality_report['checks_performed'].append('outlier_detection')
        return df
    
    def export_report(self, filepath: str):
        """Export quality report to JSON."""
        self.logger.info(f"Exporting quality report to: {filepath}")
        with open(filepath, 'w') as f:
            json.dump(self.quality_report, f, indent=2)
        self.logger.info("Quality report exported successfully")


# Example usage
if __name__ == "__main__":
    # Sample messy data
    data = {
        'user_id': [1, 2, 2, 3, 4, 5, 6, 7, 8, 9],
        'age': [25, 150, 30, -5, 35, None, 28, 45, 22, 1000],
        'email': ['user1@test.com', 'invalid-email', 'user2@test.com', 
                  'user3@test.com', 'user4@test.com', 'user5@test.com',
                  'user6@test.com', 'user7@test.com', 'user8@test.com', 'user9@test.com'],
        'salary': [50000, 60000, 55000, 70000, 65000, 80000, 75000, 90000, 5000000, 85000]
    }
    
    df = pd.DataFrame(data)
    
    # Configuration for validation
    validation_config = {
        'check_duplicates': {
            'subset': ['user_id'],
            'keep': 'first',
            'action': 'remove'
        },
        'required_columns': ['user_id', 'age', 'email'],
        'null_handling': {
            'age': 'fill_median',
            'salary': 'drop_rows'
        },
        'type_validation': {
            'user_id': 'int',
            'age': 'int',
            'salary': 'float'
        },
        'range_checks': {
            'age': {'min': 0, 'max': 120, 'action': 'remove'}
        },
        'pattern_validation': {
            'email': {
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                'action': 'flag'
            }
        },
        'outlier_detection': {
            'salary': {'method': 'iqr', 'multiplier': 1.5, 'action': 'flag'}
        }
    }
    
    # Run validation with logging
    validator = DataQualityValidator(
        log_file='data_quality.log',
        log_level='INFO'
    )
    
    cleaned_df, report = validator.validate_and_clean(df, validation_config)
    
    print("\n" + "="*60)
    print("RESULTS SUMMARY")
    print("="*60)
    print(f"Original Data Shape: {df.shape}")
    print(f"Cleaned Data Shape: {cleaned_df.shape}")
    print(f"\nChecks Performed: {len(report['checks_performed'])}")
    print(f"Issues Found: {len(report['issues_found'])}")
    print(f"\nDetailed logs written to: data_quality.log")
    print("="*60)
    
    # Export the quality report
    validator.export_report('quality_report.json')
    
    print("\nCleaned Data Preview:")
    print(cleaned_df.head())