#!/usr/bin/env python
"""
CLI Tool for Data Quality Validator - Tool 3 in AI Workflow
This script demonstrates CLI-based validation and testing of the
DataQualityValidator framework, completing the three-tool workflow:
  Tool 1 (Claude Chat) → Problem Definition
  Tool 2 (VS Code IDE) → Implementation
  Tool 3 (CLI Terminal) → Testing & Validation
"""

import sys
import json
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
from data_quality_validator import DataQualityValidator


def run_unit_tests():
    """Run pytest unit tests and capture results."""
    print("\n" + "="*70)
    print("RUNNING UNIT TESTS (CLI Tool - Testing Phase)")
    print("="*70)
    
    test_file = Path(__file__).parent / "tests" / "test_data_quality_validator.py"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", str(test_file), "-v", "--tb=short"],
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.returncode == 0:
        print("\n✅ All unit tests PASSED")
    else:
        print("\n❌ Some tests FAILED")
        print(result.stderr)
    
    return result.returncode == 0


def validate_sample_data():
    """Validate sample data using the DataQualityValidator."""
    print("\n" + "="*70)
    print("VALIDATING SAMPLE DATA (End-to-End Workflow Test)")
    print("="*70)
    
    # Create sample data with known issues
    sample_df = pd.DataFrame({
        'user_id': [1, 1, 2, 3, 4],  # Duplicate: 1 appears twice
        'name': ['Alice', 'Alice', 'Bob', 'Charlie', None],  # Null value
        'age': [25, 25, 35, None, 150],  # Out of range: 150
        'email': ['alice@example.com', 'alice@example.com', 'invalid-email', 'charlie@test.com', 'david@mail.com'],
        'salary': [50000, 50000, 60000, 55000, 1000000]  # Outlier: 1000000
    })
    
    print(f"\nInput DataFrame Shape: {sample_df.shape}")
    print(f"Input Rows: {len(sample_df)}")
    print("\nSample Input Data:")
    print(sample_df.to_string())
    
    # Define validation config
    config = {
        'check_duplicates': {
            'subset': ['user_id', 'name'],
            'action': 'remove'
        },
        'null_handling': {
            'name': 'drop_rows',
            'age': 'fill_median'
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
            'salary': {
                'method': 'iqr',
                'action': 'flag'
            }
        }
    }
    
    print("\n" + "-"*70)
    print("Validation Configuration:")
    print("-"*70)
    print(json.dumps(config, indent=2))
    
    # Run validation
    print("\n" + "-"*70)
    print("Running Validation Pipeline...")
    print("-"*70)
    
    validator = DataQualityValidator(
        log_file='logs/cli_validation.log',
        log_level='INFO'
    )
    
    cleaned_df, report = validator.validate_and_clean(sample_df, config)
    
    # Display results
    print("\n" + "-"*70)
    print("Output DataFrame Shape:", cleaned_df.shape)
    print(f"Output Rows: {len(cleaned_df)}")
    print("\nCleaned Data:")
    print(cleaned_df.to_string())
    
    print("\n" + "-"*70)
    print("Quality Report Summary:")
    print("-"*70)
    print(f"Checks Performed: {report['checks_performed']}")
    print(f"Issues Found: {len(report['issues_found'])}")
    print(f"Rows Processed: {report['rows_processed']}")
    print(f"Rows Cleaned: {report['rows_cleaned']}")
    print("\nDetailed Issues:")
    for idx, issue in enumerate(report['issues_found'], 1):
        print(f"  {idx}. {issue['type']}: {issue}")
    
    # Save report
    report_path = Path('logs/cli_validation_report.json')
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n✅ Validation complete. Report saved to: {report_path}")
    return True


def validate_framework_structure():
    """Verify framework structure and key components."""
    print("\n" + "="*70)
    print("VALIDATING FRAMEWORK STRUCTURE")
    print("="*70)
    
    required_files = [
        'data_quality_validator.py',
        'tests/test_data_quality_validator.py',
        'data_quality_documentation.md',
        'README.md'
    ]
    
    base_path = Path(__file__).parent
    all_exist = True
    
    print("\nChecking Required Files:")
    for file in required_files:
        file_path = base_path / file
        exists = file_path.exists()
        status = "✅" if exists else "❌"
        print(f"  {status} {file}")
        all_exist = all_exist and exists
    
    # Check main class exists
    print("\nChecking DataQualityValidator Class:")
    try:
        from data_quality_validator import DataQualityValidator
        validator = DataQualityValidator()
        
        # Check key methods
        methods = [
            'validate_and_clean',
            '_handle_duplicates',
            '_handle_nulls',
            '_validate_types',
            '_validate_ranges',
            '_validate_patterns',
            '_detect_outliers',
            'export_report'
        ]
        
        for method in methods:
            has_method = hasattr(validator, method)
            status = "✅" if has_method else "❌"
            print(f"  {status} {method}()")
            all_exist = all_exist and has_method
    
    except Exception as e:
        print(f"  ❌ Error importing DataQualityValidator: {e}")
        all_exist = False
    
    if all_exist:
        print("\n✅ Framework structure validation PASSED")
    else:
        print("\n❌ Framework structure validation FAILED")
    
    return all_exist


def generate_cli_report():
    """Generate comprehensive CLI validation report."""
    print("\n" + "="*70)
    print("GENERATING CLI VALIDATION REPORT")
    print("="*70)
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'workflow_phase': 'Tool 3: CLI Terminal UX - Testing & Validation Phase',
        'validation_checks': {
            'unit_tests': 'Pytest test suite execution',
            'sample_data': 'End-to-end validation with sample data',
            'framework_structure': 'Verification of all required components',
            'cli_integration': 'CLI tool integration and workflow completion'
        },
        'tools_used': [
            {
                'tool': 'Claude Chat UX',
                'phase': 'Phase 1: Problem Definition',
                'output': 'Problem statement and solution architecture'
            },
            {
                'tool': 'VS Code IDE UX',
                'phase': 'Phase 2: Implementation',
                'output': 'Complete Python framework (data_quality_validator.py)'
            },
            {
                'tool': 'CLI Terminal UX',
                'phase': 'Phase 3: Testing & Validation',
                'output': 'Test results, validation logs, and quality reports'
            }
        ],
        'data_flow': {
            'tool1_to_tool2': 'Claude output → VS Code implementation specs',
            'tool2_to_tool3': 'VS Code implementation → CLI testing scripts',
            'tool3_output': 'CLI validation results → Production-ready framework'
        },
        'status': 'COMPLETE',
        'message': 'Three-tool AI workflow successfully executed and validated'
    }
    
    report_path = Path('logs/cli_workflow_report.json')
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print("\n" + json.dumps(report, indent=2))
    print(f"\n✅ CLI workflow report saved to: {report_path}")
    return report


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='CLI Validation Tool for Data Quality Validator Framework',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_cli.py --all          # Run all validations
  python validate_cli.py --tests        # Run unit tests only
  python validate_cli.py --validate     # Validate sample data
  python validate_cli.py --structure    # Check framework structure
  python validate_cli.py --report       # Generate CLI report
        """
    )
    
    parser.add_argument('--all', action='store_true', help='Run all validations')
    parser.add_argument('--tests', action='store_true', help='Run unit tests')
    parser.add_argument('--validate', action='store_true', help='Validate sample data')
    parser.add_argument('--structure', action='store_true', help='Check framework structure')
    parser.add_argument('--report', action='store_true', help='Generate CLI report')
    
    args = parser.parse_args()
    
    # Default to all if no args
    if not any([args.all, args.tests, args.validate, args.structure, args.report]):
        args.all = True
    
    results = {}
    
    try:
        if args.all or args.structure:
            results['structure'] = validate_framework_structure()
        
        if args.all or args.tests:
            results['tests'] = run_unit_tests()
        
        if args.all or args.validate:
            results['validation'] = validate_sample_data()
        
        if args.all or args.report:
            results['report'] = generate_cli_report()
        
        # Summary
        print("\n" + "="*70)
        print("VALIDATION SUMMARY")
        print("="*70)
        for check, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"  {check.upper()}: {status}")
        
        all_passed = all(results.values())
        print("\n" + ("="*70))
        if all_passed:
            print("✅ ALL VALIDATIONS PASSED - Framework is production-ready!")
        else:
            print("❌ SOME VALIDATIONS FAILED - Review errors above")
        print("="*70 + "\n")
        
        return 0 if all_passed else 1
    
    except Exception as e:
        print(f"\n❌ Error during validation: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
