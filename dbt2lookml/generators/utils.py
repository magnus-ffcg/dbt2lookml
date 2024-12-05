import json

from dbt2lookml.log import Logger
from dbt2lookml.exceptions import CliError


class FileHandler(Logger):
    
    def read(self, file_path: str, is_json=True) -> dict:
        """Load file from disk. Default is to load as a JSON file
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary containing the JSON data OR raw contents
        """
        try:
            with open(file_path, 'r') as f:
                if is_json:
                    raw_file = json.load(f)
                else:
                    raw_file = f.read()
        except FileNotFoundError as e:
            self._logger.error(f'Could not find file at {file_path}. Use --target-dir to change the search path for the manifest.json file.')
            raise CliError('File not found')

        return raw_file
    
    def write(self, file_path: str, contents: str):
        """Write contents to a file

        Args:
            file_path (str): _description_
            contents (str): _description_

        Raises:
            CLIError: _description_
        """
        try:
            with open(file_path, 'w') as f:
                f.truncate() # Clear file to allow overwriting
                f.write(contents)
        except Exception as e:
            self._logger.error(f'Could not write file at {file_path}.')
            raise CliError('Could not write file')
        
class Sql(Logger):
    def validate_sql(self, sql: str) -> str:
        """Validate that a string is a valid Looker SQL expression.
        
        Args:
            sql: SQL expression to validate
            
        Returns:
            Validated SQL expression or None if invalid
        """
        sql = sql.strip()
        def check_if_has_dollar_syntax(sql):
            """Check if the string either has ${TABLE}.example or ${view_name}"""
            return '${' in sql and '}' in sql
        
        def check_expression_has_ending_semicolons(sql):
            """Check if the string ends with a semicolon"""
            return sql.endswith(';;')

        if check_expression_has_ending_semicolons(sql):
            self._logger.warning(f"SQL expression {sql} ends with semicolons. It is removed and added by lkml.")
            sql = sql.rstrip(';').rstrip(';').strip()

        if not check_if_has_dollar_syntax(sql):
            self._logger.warning(f"SQL expression {sql} does not contain $TABLE or $view_name")
            return None
        else:
            return sql