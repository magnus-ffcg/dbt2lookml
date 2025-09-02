import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Pure regex-based validation - no external dependencies needed

logger = logging.getLogger(__name__)


class LookMLValidationError(Exception):
    """Exception raised when LookML validation fails."""
    pass


class LookMLValidator:
    """Validates LookML syntax using modular Python functions and regex patterns."""
    patterns = {
            'atom': re.compile(r'^[-+_a-zA-Z0-9\.]+$'),
            'quoted_string': re.compile(r'^"([^"\\]|\\.)*"$'),
            'comment': re.compile(r'^\s*#.*$'),
            'whitespace': re.compile(r'^\s*$'),
            'block_type': re.compile(r'^(sql|html|expr)[-_a-zA-Z0-9]*$'),
            'object_declaration': re.compile(r'^\s*(\w+)\s*:\s*([^{]+)?\s*\{'),
            'block_declaration': re.compile(r'^\s*(sql|html|expr)[_\w]*\s*:'),
            'property_declaration': re.compile(r'^\s*(\w+)\s*:\s*(.+)$'),
            'list_element': re.compile(r'^\s*\[.*\]\s*$', re.DOTALL),
            'double_semicolon': re.compile(r'.*;;', re.DOTALL),
            'closing_brace': re.compile(r'^\s*\}\s*$'),
            'sql_table_name': re.compile(r'^\s*sql_table_name\s*:')
        }
    def __init__(self):
        """Initialize the LookML validator with regex patterns."""
        self.regex_patterns_compiled = True
    
    def _validate_with_regex(self, content: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        """Validate using regex patterns for basic syntax checking."""
        errors = []
        lines = content.split('\n')
        
        brace_stack = []
        in_sql_block = False
        current_block_type = None
        
        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            
            # Skip empty lines and comments
            if not stripped or self.patterns['comment'].match(line):
                continue
            
            # Check for top-level object declarations (view, explore, etc.)
            if '{' in stripped and not in_sql_block:
                # This is likely an object declaration
                if ':' in stripped:
                    parts = stripped.split(':', 1)
                    obj_type = parts[0].strip()
                    # Only count as object if it's a known LookML object type
                    if obj_type in ['view', 'explore', 'dashboard', 'dimension', 'measure', 'filter', 'dimension_group', 'join']:
                        # Check if this is an empty declaration (e.g., "view: name {}")
                        if stripped.endswith('{}'):
                            # Empty declaration - valid, no need to track braces
                            continue
                        else:
                            brace_stack.append((line_num, obj_type))
                            current_block_type = obj_type
                            continue
            
            # Check for closing braces
            if self.patterns['closing_brace'].match(stripped):
                if not brace_stack:
                    errors.append(f"Line {line_num}: Unmatched closing brace")
                else:
                    brace_stack.pop()
                    current_block_type = None
                continue
            
            # Check for SQL-like declarations that need ;;
            if any(stripped.startswith(prefix) for prefix in ['sql_table_name:', 'sql:', 'html:', 'expr:']):
                if not stripped.endswith(';;'):
                    # Look ahead to see if ;; is on next lines
                    found_terminator = False
                    for next_line_num in range(line_num, min(line_num + 10, len(lines))):
                        if next_line_num < len(lines) and ';;' in lines[next_line_num]:
                            found_terminator = True
                            break
                    
                    if not found_terminator:
                        errors.append(f"Line {line_num}: SQL/HTML/Expression block should end with ;;")
                continue
            
            # Basic property validation for simple properties
            if ':' in stripped and '{' not in stripped and '}' not in stripped:
                # This is a simple property declaration - should be valid
                continue
        
        # Check for unmatched braces
        if brace_stack:
            unmatched_lines = [str(line_num) for line_num, _ in brace_stack]
            errors.append(f"Unmatched opening braces at lines: {', '.join(unmatched_lines)}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'method': 'regex'
        }
    
    
    
    def validate_lookml_string(self, content: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate a LookML string using comprehensive regex patterns.
        
        Args:
            content: LookML content to validate
            file_path: Optional file path for error reporting
            
        Returns:
            Dict with validation results
        """
        # Use only regex validation - fast and comprehensive
        result = self._validate_with_regex(content, file_path)
        result['file'] = file_path or 'Unknown file'
        result['methods_used'] = [result['method']]
        
        return result
    
    def validate_lookml_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Validate a LookML file for syntax errors.
        
        Args:
            file_path: Path to the LookML file
            
        Returns:
            Dict with validation results
        """
        try:
            content = file_path.read_text(encoding='utf-8')
            return self.validate_lookml_string(content, str(file_path))
        except Exception as e:
            return {
                'valid': False,
                'errors': [f"Failed to read file {file_path}: {str(e)}"],
                'parsed': None
            }
    
    def validate_directory(self, directory: Path, pattern: str = "*.lkml") -> Dict[str, Any]:
        """
        Validate all LookML files in a directory.
        
        Args:
            directory: Directory containing LookML files
            pattern: File pattern to match (default: "*.lkml")
            
        Returns:
            Dict with validation results:
            {
                'valid': bool,
                'total_files': int,
                'valid_files': int,
                'invalid_files': int,
                'results': Dict[str, Dict] - per-file results
            }
        """
        if not directory.exists():
            return {
                'valid': False,
                'total_files': 0,
                'valid_files': 0,
                'invalid_files': 0,
                'results': {},
                'errors': [f"Directory does not exist: {directory}"]
            }
        
        files = list(directory.glob(pattern))
        if not files:
            # Also check subdirectories
            files = list(directory.rglob(pattern))
        
        if not files:
            return {
                'valid': True,
                'total_files': 0,
                'valid_files': 0,
                'invalid_files': 0,
                'results': []
            }
        
        results = []
        valid_count = 0
        invalid_count = 0
        
        # Use concurrent processing with 4 workers for better performance
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all validation tasks
            future_to_file = {
                executor.submit(self._validate_single_file, file_path, directory): file_path 
                for file_path in files
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['valid']:
                        valid_count += 1
                    else:
                        invalid_count += 1
                        
                except Exception as e:
                    # Handle worker errors
                    relative_path = str(file_path.relative_to(directory))
                    error_result = {
                        'valid': False,
                        'errors': [f"Validation error: {str(e)}"],
                        'file': relative_path,
                        'methods_used': ['error']
                    }
                    results.append(error_result)
                    invalid_count += 1
        
        return {
            'valid': invalid_count == 0,
            'total_files': len(files),
            'valid_files': valid_count,
            'invalid_files': invalid_count,
            'results': results
        }
    
    def _validate_single_file(self, file_path: Path, directory: Path) -> Dict[str, Any]:
        """Validate a single file and return result with relative path."""
        result = self.validate_lookml_file(file_path)
        relative_path = str(file_path.relative_to(directory))
        result['file'] = relative_path
        return result
    
    def print_validation_report(self, validation_result: Dict[str, Any], verbose: bool = False):
        """Log a concise validation report."""
        # Normalize single file results to match directory format
        if 'total_files' not in validation_result:
            total = 1
            valid = 1 if validation_result['valid'] else 0
            results = [validation_result] if not validation_result['valid'] else []
        else:
            total = validation_result['total_files']
            valid = validation_result['valid_files']
            results = validation_result.get('results', [])
        
        invalid = total - valid
        success_rate = (valid / total * 100) if total > 0 else 0
        
        # One-line summary
        logger.info(f"LookML validation: {valid}/{total} files valid ({success_rate:.1f}%)")
        
        # Show errors only if verbose and there are failures
        if verbose and invalid > 0:
            for file_result in results:
                if not file_result['valid']:
                    file_path = file_result['file']
                    logger.warning(f"{file_path}: {len(file_result['errors'])} error(s)")
                    for error in file_result['errors']:
                        logger.warning(f"  - {error}")


def validate_generated_lookml(output_directory: Path, verbose: bool = False) -> bool:
    """
    Validate all generated LookML files in the output directory.
    
    Args:
        output_directory: Path to the directory containing generated LookML
        verbose: Whether to show detailed error messages
        
    Returns:
        True if all files are valid, False otherwise
    """
    validator = LookMLValidator()
    result = validator.validate_directory(output_directory)
    validator.print_validation_report(result, verbose)
    
    return result['valid']


if __name__ == "__main__":
    """CLI interface for validation."""
    import argparse
    import sys

    # Configure logging for CLI
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    parser = argparse.ArgumentParser(description="Validate LookML files")
    parser.add_argument("path", help="Path to LookML file or directory")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed error messages")
    parser.add_argument("--pattern", default="*.lkml", help="File pattern to match (default: *.lkml)")
    
    args = parser.parse_args()
    path = Path(args.path)
    
    validator = LookMLValidator()
    
    if path.is_file():
        result = validator.validate_lookml_file(path)
        validator.print_validation_report(result, args.verbose)
        sys.exit(0 if result['valid'] else 1)
    elif path.is_dir():
        result = validator.validate_directory(path, args.pattern)
        validator.print_validation_report(result, args.verbose)
        sys.exit(0 if result['valid'] else 1)
    else:
        logger.error(f"Path does not exist: {path}")
        sys.exit(1)
