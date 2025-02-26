
from .config import *

import logging
import os
import duckdb
import pandas as pd
import glob
import psutil


class DeltaLens:
    """
    A class for comparing datasets using DuckDB as the comparison engine.
    The DeltaLens class facilitates data comparison between two datasets for multiple entities,
    managing the database connections and comparison workflow. It supports both in-memory and
    persistent database operations.
    Parameters
    ----------
    runName : str
        A unique identifier for this comparison run
    entityConfig : Config
        Configuration object containing entity definitions and comparison settings
    persistent : bool, optional
        If True, creates a persistent DuckDB database file, otherwise uses in-memory database
        (default is False)
    persist_path : str, optional
        Directory path where the persistent database file will be stored
        (default is current directory '.')
    Attributes
    ----------
    config : Config
        Stores the entity configuration
    runName : str
        Name of the current comparison run
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database
    duck_db_fileName : str
        Path to the DuckDB database file or memory identifier
    Methods
    -------
    execute(continue_on_error=True)
        Executes the comparison process for all configured entities
    Notes
    -----
    - The class creates a results table named 'entity_compare_results' to track comparison outcomes
    - The execute() method can only be called once per instance
   
    """
    def __init__(self, runName: str, entityConfig: Config, persistent = False, persist_path = '.'):
        self.config = entityConfig
       
        self.runName = runName
        self.logger = logging.getLogger(self.__class__.__name__ + "[" + runName + "]")
        clean_name = f'{''.join(c for c in runName if c.isalnum() or c in '._- ')}'

        if persistent:
            self.duck_db_fileName =f'{os.path.join(persist_path, (clean_name +  ".duckdb"))}'
        else:
            self.duck_db_fileName =f':memory:{clean_name}'
            
            
        memory_gb = int(psutil.virtual_memory().total * 0.7 / (1024**3))
       
        self.logger.info(f'setting duckdb memory_limit to {memory_gb}GB')
        self.con = duckdb.connect(self.duck_db_fileName, config={'preserve_insertion_order': False, 'memory_limit': f'{memory_gb}GB'})

        self.logger.info(f"Connected to DuckDB database @: {self.duck_db_fileName}")
      

    def __populateDefaults(self):
        if self.config.defaults and self.config.defaults.leftSideTitle:
            for entity in self.config.entities:
                entity.leftSide.title = self.config.defaults.leftSideTitle
        if self.config.defaults and self.config.defaults.rightSideTitle:
            for entity in self.config.entities:
                entity.rightSide.title = self.config.defaults.rightSideTitle
        if self.config.defaults and self.config.defaults.filePattenGlobTemplate:

             for entity in self.config.entities:
            # find first matching file for left side
                left_glob_pattern = self.config.defaults.filePattenGlobTemplate.format(entityName=entity.entityName, title=entity.leftSide.title)
                left_matches = glob.glob(left_glob_pattern)
                if not left_matches:
                    raise FileNotFoundError(f"Executing file glob for [{entity.entityName}] entity: No files found matching pattern: {left_glob_pattern}")
                entity.leftSide.inputFile = left_matches[0]

                # find first matching file for right side 
                right_glob_pattern = self.config.defaults.filePattenGlobTemplate.format(entityName=entity.entityName, title=entity.rightSide.title)
                right_matches = glob.glob(right_glob_pattern)
                if not right_matches:
                    raise FileNotFoundError(f"Executing file glob for [{entity.entityName}] entity: No files found matching pattern: {right_glob_pattern}")
                entity.rightSide.inputFile = right_matches[0]

    def __loadExternalDatasets(self):
        if self.config.reference_datasets:
            for dataset in self.config.reference_datasets:
                self.logger.info(f"Loading reference dataset: {dataset.datasetName} from {dataset.inputFile}")
                if not os.path.exists(dataset.inputFile):
                     raise FileNotFoundError(f"file not found at: {dataset.inputFile}")
                self.con.execute(f"CREATE TABLE {dataset.datasetName} AS SELECT * FROM read_csv_auto('{dataset.inputFile}')")

    def __createResultsTable(self):
        self.con.execute("CREATE TABLE entity_compare_results (entity VARCHAR PRIMARY KEY, rows_left INT, rows_right INT, rows_fully_matched INT, error_text VARCHAR, success INT);")
    
    def execute(self, continue_on_error = True):      
        if hasattr(self, '_has_executed'):
            raise ValueError("Execute method has already been called")
        self.__populateDefaults()
        Config.Validate(self.config)
        self.__createResultsTable()
        self.__loadExternalDatasets()
        for entity in self.config.entities:
            self.logger.info(f"Processing entity: [{entity.entityName}]")
            try:
                equityComparer = EntityComparer(self.con, entity)
                equityComparer.runcompare()
                # Log success
                self.con.execute(
                    """INSERT INTO entity_compare_results 
                    (entity, rows_left, rows_right, rows_fully_matched, error_text, success)
                    SELECT ?, 
                        COUNT(*) FILTER (WHERE _exists_left), 
                        COUNT(*) FILTER (WHERE _exists_right),
                        COUNT(*) FILTER (WHERE _full_match),
                        NULL,
                        1
                    FROM {}_compare""".format(entity.entityName), 
                    [entity.entityName]
                )
                self.logger.info(f"Completed processing entity: [{entity.entityName}]")
            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"Error processing entity [{entity.entityName}]: {e}")
                # Log failure
                self.con.execute(
                    """INSERT INTO entity_compare_results 
                    (entity, rows_left, rows_right, rows_fully_matched, error_text, success)
                    VALUES (?, NULL, NULL, NULL, ?, 0)""",
                    [entity.entityName, error_msg]
                )
                if not continue_on_error:
                    raise

        self._has_executed = True



class EntityComparer:
    """
    A class for comparing two datasets (left and right side) with the same structure and primary keys.
    This class handles data comparison between two CSV files, applying optional transformations,
    and generating detailed comparison results including field-level matching statistics.
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        A DuckDB connection object for database operations
    entity : Entity
        An Entity object containing comparison configuration details including:
        - entityName: Name of the entity being compared
        - leftSide: Configuration for left dataset (input file, transformations)
        - rightSide: Configuration for right dataset (input file, transformations)
        - primaryKeys: List of columns that uniquely identify records
        - excludeColumns: List of columns to exclude from comparison
    Attributes
    ----------
    logger : Logger
        Logger instance for the class
    con : duckdb.DuckDBPyConnection
        DuckDB connection object
    entity : Entity
        Entity configuration object
    leftSideInputTable : str
        Name of the table containing left side data
    rightSideInputTable : str
        Name of the table containing right side data
    leftSideInputTableTransformed : str, optional
        Name of the transformed left side table (if transformation is applied)
    rightSideInputTableTransformed : str, optional
        Name of the transformed right side table (if transformation is applied)
    Methods
    -------
    runcompare()
        Executes the comparison process and generates results:
        - Loads data from CSV files into DuckDB tables
        - Applies transformations if specified
        - Validates data types and primary keys
        - Performs full outer join comparison
        - Creates comparison results table and summary statistics
    Raises
    ------
    FileNotFoundError
        If input CSV files are not found
    ValueError
        If primary keys are missing in tables
        If column data types don't match between tables
        If required columns are missing in either table
        """
  
    def __init__(self, con:duckdb.DuckDBPyConnection, entity: Entity):
        self.logger = logging.getLogger(self.__class__.__name__ + "[" + entity.entityName + "]")
        self.con = con
        self.entity = entity
        self.leftSideInputTable = f"{self.entity.entityName}_{self.entity.leftSide.title}"
        self.rightSideInputTable = f"{self.entity.entityName}_{self.entity.rightSide.title}"
        if self.entity.leftSide.transform:
            self.leftSideInputTableTransformed = f"{self.entity.entityName}_{self.entity.leftSide.title}_transformed"

        if self.entity.rightSide.transform:
            self.rightSideInputTableTransformed = f"{self.entity.entityName}_{self.entity.rightSide.title}_transformed"


    def __applyLeftTransform(self):
        self.logger.info(f"Applying left side transform: {self.entity.leftSide.transform.query}")
                 
        if self.entity.leftSide.transform.cached:
            create_statement = f"CREATE TABLE {self.leftSideInputTableTransformed} AS {self.entity.leftSide.transform.query}"
            self.con.execute(create_statement)
            self.con.execute(f"ALTER TABLE {self.leftSideInputTableTransformed} ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")
        else:
            create_statement = f"CREATE VIEW {self.leftSideInputTableTransformed} AS {self.entity.leftSide.transform.query}"   
            self.con.execute(create_statement)

        self.leftSideInputTable = self.leftSideInputTableTransformed

        
        
    def __applyRightTransform(self):
        self.logger.info(f"Applying right side transform: {self.entity.rightSide.transform.query}")
                 
        if self.entity.rightSide.transform.cached:
            create_statement = f"CREATE TABLE {self.rightSideInputTableTransformed} AS {self.entity.rightSide.transform.query}"
            self.con.execute(create_statement)
            self.con.execute(f"ALTER TABLE {self.rightSideInputTableTransformed} ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")
        else:
            create_statement = f"CREATE VIEW {self.rightSideInputTableTransformed} AS {self.entity.rightSide.transform.query}"   
            self.con.execute(create_statement)

        self.rightSideInputTable = self.rightSideInputTableTransformed
       

  
    def runcompare(self):
       
        self.__create_left_side_table()     
        self.__create_right_side_table()

        if self.entity.leftSide.transform:
            self.__applyLeftTransform()
        if self.entity.rightSide.transform:
            self.__applyRightTransform()

        # get column names from both tables
        left_columns, right_columns = self.__extract_columns()

        # verify column data types match between both tables
        self.__validate_data_types(left_columns, right_columns)


        # verify primary keys exist in both tables
        for pk in self.entity.primaryKeys:
            if pk not in left_columns:
                raise ValueError(f"Primary key '{pk}' not found in left table [{self.leftSideInputTable}] columns: {left_columns}")
            if pk not in right_columns:
                raise ValueError(f"Primary key '{pk}' not found in right table [{self.rightSideInputTable}] columns: {right_columns}")


        # Build join conditions for multiple primary keys
        join_conditions = []
        for pk in self.entity.primaryKeys:
            join_conditions.append(f'{self.leftSideInputTable}."{pk}" = {self.rightSideInputTable}."{pk}"')
        
        join_condition = " AND ".join(join_conditions)
        full_outer_join = f"FROM {self.leftSideInputTable} FULL OUTER JOIN {self.rightSideInputTable} ON {join_condition}"

        # Get all non-primary key columns for comparison
        comparison_columns = set(left_columns + right_columns) - set(self.entity.primaryKeys)
        if self.entity.excludeColumns is not None:
             comparison_columns = comparison_columns - set(self.entity.excludeColumns)

        # Build column comparisons for the SELECT clause
        column_expressions = []
        match_columns = []


        for col in self.entity.primaryKeys:
            column_expressions.append(f'coalesce({self.leftSideInputTable}."{col}", {self.rightSideInputTable}."{col}") as "{col}"')

        for col in comparison_columns:
            # Check if column exists in both tables
            in_left = col in left_columns
            in_right = col in right_columns
            
            if in_left and in_right:
                column_expressions.append(f'{self.leftSideInputTable}."{col}" as "{col}_left"')
                column_expressions.append(f'{self.rightSideInputTable}."{col}" as "{col}_right"')
                column_expressions.append(f'({self.leftSideInputTable}."{col}" = {self.rightSideInputTable}."{col}" OR ({self.leftSideInputTable}."{col}" IS NULL AND {self.rightSideInputTable}."{col}" IS NULL)) as "{col}_match"')
                match_columns.append(f'"{col}_match"')
            else:
                raise ValueError(f"Column '{col}' not found in both tables: left table= {self.leftSideInputTable}.found={in_left}, right table={self.rightSideInputTable}.found={in_right}")
        if self.entity.excludeColumns is not None:
            for col in self.entity.excludeColumns:
                in_left = col in left_columns
                in_right = col in right_columns

                if in_left:
                    column_expressions.append(f'{self.leftSideInputTable}."{col}" as "{col}_left"')
                if in_right:
                    column_expressions.append(f'{self.rightSideInputTable}."{col}" as "{col}_right"')


        exists_left_expression = [f'{self.leftSideInputTable}."{col}"  IS NOT NULL ' for col in self.entity.primaryKeys]
        exists_left_expression = " AND ".join(exists_left_expression)

        column_expressions.append(f"({exists_left_expression}) as _exists_left")


        exists_right_expression = [f'{self.rightSideInputTable}."{col}"  IS NOT NULL ' for col in self.entity.primaryKeys]
        exists_right_expression = " AND ".join(exists_right_expression)

        column_expressions.append(f"({exists_right_expression}) as _exists_right")

        select_clause = ", ".join(column_expressions)

        compare_sql_statement = f"SELECT {select_clause} {full_outer_join}"

        match_columns = [ col  + "=1" for col in match_columns]

        match_columns_str = " AND ".join(match_columns)

        cte_statement = f"WITH comparison AS ({compare_sql_statement}) SELECT *, ({match_columns_str}) as _full_match  FROM comparison"
        
        self.logger.info(cte_statement)

        view_name = f"{self.entity.entityName}_compare_view"

        view_statement = f"CREATE VIEW {view_name} AS {cte_statement}"
        
        self.con.execute(view_statement)

        create_result_table_statement = f"CREATE TABLE {self.entity.entityName}_compare AS SELECT * FROM {view_name}"
        
        self.con.execute(create_result_table_statement)

        self.con.execute(f"ALTER TABLE  {self.entity.entityName}_compare ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")

        self.__create_field_summary_table(match_columns)


    def __create_right_side_table(self):
        self.logger.info(f"Loading data into right side input table: {self.rightSideInputTable} from {self.entity.rightSide.inputFile}")
        if not os.path.exists(self.entity.rightSide.inputFile):
            raise FileNotFoundError(f"Right side input file not found: {self.entity.rightSide.inputFile}")
       
        self.con.execute(f"CREATE TABLE  {self.rightSideInputTable} AS SELECT * FROM read_csv_auto('{self.entity.rightSide.inputFile}')")  
        self.logger.info(f"adding primary key to right side input table: {self.rightSideInputTable}")
        self.con.execute(f"ALTER TABLE {self.rightSideInputTable} ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")

    def __create_left_side_table(self):
        self.logger.info(f"Loading data into left side input table: {self.leftSideInputTable} from {self.entity.leftSide.inputFile}")
        if not os.path.exists(self.entity.leftSide.inputFile):
            raise FileNotFoundError(f"Left side input file not found: {self.entity.leftSide.inputFile}")
       
        self.con.execute(f"CREATE TABLE  {self.leftSideInputTable} AS SELECT * FROM read_csv_auto('{self.entity.leftSide.inputFile}')")
        self.logger.info(f"adding primary key to left side input table: {self.leftSideInputTable}")
        self.con.execute(f"ALTER TABLE {self.leftSideInputTable} ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")

    def __extract_columns(self):
        left_columns = self.con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.leftSideInputTable}'").fetchall()
        right_columns = self.con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.rightSideInputTable}'").fetchall()

        # convert from list of tuples to list of strings
        left_columns = [col[0] for col in left_columns]
        right_columns = [col[0] for col in right_columns]
        return left_columns,right_columns

    def __validate_data_types(self, left_columns, right_columns):
        left_dtypes = self.con.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{self.leftSideInputTable}'").fetchdf()
        right_dtypes = self.con.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{self.rightSideInputTable}'").fetchdf()

        # create dictionaries mapping column names to data types
        left_dtype_dict = dict(zip(left_dtypes['column_name'], left_dtypes['data_type']))
        right_dtype_dict = dict(zip(right_dtypes['column_name'], right_dtypes['data_type']))

        # check matching columns have same data type
        common_columns = set(left_columns) & set(right_columns)
        for col in common_columns:
            if left_dtype_dict[col] != right_dtype_dict[col]:
                raise ValueError(f"Data type mismatch for column '{col}': left table= {self.leftSideInputTable}.{left_dtype_dict[col]}, right table={self.rightSideInputTable}.{right_dtype_dict[col]}")

    def __create_field_summary_table(self, match_columns):
        summary_query = """
            WITH match_counts AS (
                SELECT
                    COUNT(*) as total_count,
                    """ + \
                    ",            ".join([f'SUM(CASE WHEN {col} THEN 1 ELSE 0 END) as {col[:-9]}_matches"' for col in match_columns]) + \
        """
                FROM {entity}_compare WHERE _exists_left = 1 and _exists_right = 1 
            )
            SELECT
                """ + \
                " UNION ALL SELECT ".join([
                    f'\'{col[:-9][1:]}\' as field, total_count as total, {col[:-9]}_matches" as matches, ' + \
                    f'ROUND(CAST({col[:-9]}_matches" AS FLOAT) / total_count * 100, 2) as match_percentage FROM match_counts'
                    for col in match_columns
                ])

        summary_view_statement = f"CREATE VIEW {self.entity.entityName}_compare_field_summary AS {summary_query.format(entity=self.entity.entityName)}"
        self.logger.info(summary_view_statement)
        self.con.execute(summary_view_statement)
        # if transform query is provided, execute it

    

   