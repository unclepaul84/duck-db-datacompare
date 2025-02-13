
from config import *

import logging
import os
import duckdb
import pandas as pd


class DeltaLens:
    """
    
    """
    def __init__(self, runName: str, entityConfig: Config):
        self.config = entityConfig
        self.runName = runName
        self.logger = logging.getLogger(self.__class__.__name__ + "." + runName)
        self.duck_db_fileName = f'{''.join(c for c in runName if c.isalnum() or c in '._- ')}.duckdb'
        self.con = duckdb.connect(self.duck_db_fileName)
        self.logger.info(f"Connected to DuckDB database @: {self.duck_db_fileName}")

    def execute(self):
        """
        """
        for entity in self.config.entities:
            self.logger.info(f"Processing entity: {entity.entityName}")
            equityComparer = EntityComparer(self.con, entity)
            equityComparer.runcompare()
        


class EntityComparer:
    """
    
    """
    def __init__(self, con:duckdb.DuckDBPyConnection, entity: Entity):
        self.logger = logging.getLogger(self.__class__.__name__ + "." + entity.entityName)
        self.con = con
        self.entity = entity
        self.leftSideInputTable = f"{self.entity.entityName}_{self.entity.leftSide.title}"
        self.rightSideInputTable = f"{self.entity.entityName}_{self.entity.rightSide.title}"
        if self.entity.leftSide.transform:
            self.leftSideInputTableTransformed = f"{self.entity.entityName}_{self.entity.leftSide.title}_transformed"

        if self.entity.rightSide.transform:
            self.rightSideInputTableTransformed = f"{self.entity.entityName}_{self.entity.rightSide.title}_transformed"


    def runcompare(self):
        self.logger.info(f"Loading data into left side input table: {self.leftSideInputTable} from {self.entity.leftSide.inputFile}")
        self.con.execute(f"CREATE TABLE  {self.leftSideInputTable} AS SELECT * FROM read_csv_auto('{self.entity.leftSide.inputFile}')")
        self.logger.info(f"Loading data into right side input table: {self.rightSideInputTable} from {self.entity.rightSide.inputFile}")
        self.con.execute(f"CREATE TABLE  {self.rightSideInputTable} AS SELECT * FROM read_csv_auto('{self.entity.rightSide.inputFile}')")

        self.logger.info(f"adding primary key to left side input table: {self.leftSideInputTable}")
        self.con.execute(f"ALTER TABLE {self.leftSideInputTable} ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")
        
        self.logger.info(f"adding primary key to right side input table: {self.rightSideInputTable}")
        self.con.execute(f"ALTER TABLE {self.rightSideInputTable} ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")

        # get column names from both tables
        left_columns = self.con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.leftSideInputTable}'").fetchall()
        right_columns = self.con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.rightSideInputTable}'").fetchall()

        # convert from list of tuples to list of strings
        left_columns = [col[0] for col in left_columns]
        right_columns = [col[0] for col in right_columns]


        # verify column data types match between both tables
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
            
        # verify primary keys exist in both tables
        for pk in self.entity.primaryKeys:
            if pk not in left_columns:
                raise ValueError(f"Primary key '{pk}' not found in left table [{self.leftSideInputTable}] columns: {left_columns}")
            if pk not in right_columns:
                raise ValueError(f"Primary key '{pk}' not found in right table [{self.rightSideInputTable}] columns: {right_columns}")


        # Build join conditions for multiple primary keys
        join_conditions = []
        for pk in self.entity.primaryKeys:
            join_conditions.append(f"{self.leftSideInputTable}.{pk} = {self.rightSideInputTable}.{pk}")
        
        join_condition = " AND ".join(join_conditions)
        full_outer_join = f"FROM {self.leftSideInputTable} FULL OUTER JOIN {self.rightSideInputTable} ON {join_condition}"

        # Get all non-primary key columns for comparison
        comparison_columns = set(left_columns + right_columns) - set(self.entity.primaryKeys) - set(self.entity.excludeColumns)

        # Build column comparisons for the SELECT clause
        column_expressions = []
        match_columns = []


        for col in self.entity.primaryKeys:
            column_expressions.append(f"coalesce({self.leftSideInputTable}.{col}, {self.rightSideInputTable}.{col}) as {col}")

        for col in comparison_columns:
            # Check if column exists in both tables
            in_left = col in left_columns
            in_right = col in right_columns
            
            if in_left and in_right:
                column_expressions.append(f"{self.leftSideInputTable}.{col} as {col}_left")
                column_expressions.append(f"{self.rightSideInputTable}.{col} as {col}_right")
                column_expressions.append(f"({self.leftSideInputTable}.{col} = {self.rightSideInputTable}.{col} OR {self.leftSideInputTable}.{col} IS NULL AND {self.rightSideInputTable}.{col} IS NULL) as {col}_match")
                match_columns.append(f"{col}_match")
            else:
                raise ValueError(f"Column '{col}' not found in both tables: left table= {self.leftSideInputTable}.{in_left}, right table={self.rightSideInputTable}.{in_right}")

        for col in self.entity.excludeColumns:
            in_left = col in left_columns
            in_right = col in right_columns

            if in_left:
                column_expressions.append(f"{self.leftSideInputTable}.{col} as {col}_left")
            if in_right:
                column_expressions.append(f"{self.rightSideInputTable}.{col} as {col}_right")


        exists_left_expression = [f"{self.leftSideInputTable}.{col}  IS NOT NULL " for col in self.entity.primaryKeys]
        exists_left_expression = " AND ".join(exists_left_expression)

        column_expressions.append(f"({exists_left_expression}) as _exists_left")


        exists_right_expression = [f"{self.rightSideInputTable}.{col}  IS NOT NULL " for col in self.entity.primaryKeys]
        exists_right_expression = " AND ".join(exists_right_expression)

        column_expressions.append(f"({exists_right_expression}) as _exists_right")

        select_clause = ", ".join(column_expressions)

        compare_sql_statement = f"SELECT {select_clause} {full_outer_join}"

        match_columns = [col + "=1" for col in match_columns]

        match_columns_str = " AND ".join(match_columns)

        cte_statement = f"WITH comparison AS ({compare_sql_statement}) SELECT *, ({match_columns_str}) as _full_match  FROM comparison"
        
        self.logger.info(cte_statement)

        view_name = f"{self.entity.entityName}_compare_view"

        view_statement = f"CREATE VIEW {view_name} AS {cte_statement}"
        
        self.con.execute(view_statement)

        create_result_table_statement = f"CREATE TABLE {self.entity.entityName}_compare AS SELECT * FROM {view_name}"
        
        self.con.execute(create_result_table_statement)

        self.con.execute(f"ALTER TABLE  {self.entity.entityName}_compare ADD PRIMARY KEY ({','.join(self.entity.primaryKeys)})")

        result_statement = f"SELECT * FROM {self.entity.entityName}_compare"

        comparison_result = self.con.execute(result_statement).fetchdf()

        # Generate summary statistics for each field using SQL
       
        summary_query = """
            WITH match_counts AS (
                SELECT
                    COUNT(*) as total_count,
                    """ + \
                    ",\n            ".join([f"SUM(CASE WHEN {col} THEN 1 ELSE 0 END) as {col[:-6]}_matches" for col in match_columns]) + \
        """
                FROM {entity}_compare
            )
            SELECT
                """ + \
                " UNION ALL SELECT ".join([
                    f"'{col[:-8]}' as field, total_count as total, {col[:-6]}_matches as matches, " + \
                    f"ROUND(CAST({col[:-6]}_matches AS FLOAT) / total_count * 100, 2) as match_percentage FROM match_counts"
                    for col in match_columns
                ])

        summary_view_statement = f"CREATE VIEW {self.entity.entityName}_compare_summary AS {summary_query.format(entity=self.entity.entityName)}"

        self.con.execute(summary_view_statement)

        summary_results = self.con.execute(f"select * from {self.entity.entityName}_compare_summary").fetchdf()
        self.logger.info("\nMatch Summary:")
        self.logger.info(summary_results)  
        self.logger.info(comparison_result)
        # if transform query is provided, execute it

    

   