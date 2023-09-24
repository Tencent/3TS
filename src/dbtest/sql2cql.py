# /*
#  * Tencent is pleased to support the open source community by making 3TS available.
#  *
#  * Copyright (C) 2022 THL A29 Limited, a Tencent company.  All rights reserved. The below software
#  * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
#  * Tencent Modifications are Copyright (C) THL A29 Limited.
#  *
#  * Author:  zhenhuazhuang (zhenhua.zhuang@qq.com)
#  *
#  */

# This Python script is designed to convert SQL statements to CQL (Cassandra Query Language) statements. 
# It currently handles only the INSERT statements and converts them to the full INSERT INTO ... VALUES ... format.
# For example, it converts "INSERT INTO mytab VALUES (1, 20);" to "INSERT INTO mytab (k,v) VALUES (1, 20);"

import os
import re

"""
Extracts column names from a SQL column definition string.

@param column_str The SQL column definition string.
@return A list of column names extracted from the input string.
"""
def extract_columns(column_str):
    depth = 0
    start = 0
    columns = []

    for i, c in enumerate(column_str):
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
        elif c == ',' and depth == 0:
            columns.append(column_str[start:i].strip())
            start = i + 1

    # Add the last column name
    columns.append(column_str[start:].strip())

    # Extract column names from column definitions
    column_names = []
    for column in columns:
        if not column.lower().startswith(('primary key', 'key')):
            column_name = column.split()[0]
            column_names.append(column_name)
    return column_names

class SQLFileProcessor:
    def __init__(self, input_dir, output_dir):
        """
        Initializes the SQL file processor.

        :param input_dir: The path to the input directory.
        :param output_dir: The path to the output directory.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        # Check and create the output folder if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def process_file(self, filename):
        """
        Processes a single SQL file.

        :param filename: The name of the SQL file to process.
        """
        input_filepath = os.path.join(self.input_dir, filename)
        output_filepath = os.path.join(self.output_dir, filename)

        with open(input_filepath, "r") as file:
            lines = file.readlines()

        table_columns = {}
        # Process each line
        for i, line in enumerate(lines):
            line = line.strip()
            parts = line.split('-')
            if len(parts) >= 3:
                actual_line = parts[2]
                if actual_line.lower().startswith("create table"):
                    match = re.match(r"create table (\w+)\s?\((.+?)\)", actual_line, re.I)
                    current_table = match.group(1)
                    # Locate the positions of the first left parenthesis and the last right parenthesis
                    start_idx = actual_line.find('(')
                    end_idx = actual_line.rfind(')')
                    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
                        # Extract the string between them
                        columns_str = actual_line[start_idx + 1:end_idx]
                        table_name = actual_line[len("create table "):start_idx].strip()
                        columns = extract_columns(columns_str)
                        table_columns[current_table] = columns
                elif actual_line.lower().startswith("insert into"):
                    match = re.match(r"insert into (\w+) values", actual_line, re.I)
                    if match:
                        table = match.group(1)
                        columns = table_columns.get(table, [])
                        columns_str = ",".join(columns)
                        if line.find("values") != -1:
                            new_line = line.replace("values", f"({columns_str}) values")
                        else:
                            new_line = line.replace("VALUES", f"({columns_str}) VALUES") 
                        lines[i] = new_line + '\n'
                        
        with open(output_filepath, "w") as file:
            file.writelines(lines)

    def process_all_files(self):
        """
        Processes all SQL files in the input directory.
        """
        for filename in os.listdir(self.input_dir):
            if filename.endswith(".txt"):
                self.process_file(filename)

input_directory = "t/pg_old"
output_directory = "t/pg"

processor = SQLFileProcessor(input_directory, output_directory)
processor.process_all_files()
