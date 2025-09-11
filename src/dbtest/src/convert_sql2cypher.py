import os
import re

def sql_to_cypher(sql_line: str) -> str:
    line = sql_line.strip()
    if not line:
        return line

    # 保留 serializable 部分原样
    if line.startswith("serializable") or line.startswith("}"):
        return line
    if re.match(r"^\d+-\d*,?\d*", line) and not any(kw in line.lower() for kw in ["drop", "create", "insert", "select", "update", "begin", "commit"]):
        return line

    # 提取前缀
    if "-" in line:
        prefix_str, stmt = line.split("-", 2)[0] + "-" + line.split("-", 2)[1], line.split("-", 2)[2]
    else:
        return line
    stmt = stmt.strip().rstrip(";")

    # 获取表名，作为标签（例如 t1 → :T1）
    m_table = re.search(r"\bfrom\s+(\w+)|\binto\s+(\w+)|\bupdate\s+(\w+)|\btable\s+(\w+)", stmt, re.I)
    label = "Test"
    if m_table:
        for g in m_table.groups():
            if g:
                label = g.capitalize()

    # DROP TABLE
    if stmt.lower().startswith("drop table"):
        return f"{prefix_str}-MATCH (n:{label}) DETACH DELETE n;"

    # CREATE TABLE
    if stmt.lower().startswith("create table"):
        return f"{prefix_str}-// CREATE TABLE ignored in Cypher"

    # INSERT
    m = re.match(r"insert into \w+ values\s*\((\d+),\s*(\d+)\)", stmt, re.I)
    if m:
        k, v = m.groups()
        return f"{prefix_str}-CREATE (:{label} {{k: {k}, v: {v}}});"

    # SELECT
    if stmt.lower().startswith("select"):
        if "where" in stmt.lower():
            m = re.match(r"select \* from \w+ where k\s*=\s*(\d+)", stmt, re.I)
            if m:
                k = m.group(1)
                return f"{prefix_str}-MATCH (n:{label} {{k: {k}}}) RETURN n;"
        if "order by" in stmt.lower():
            return f"{prefix_str}-MATCH (n:{label}) RETURN n ORDER BY n.k;"

    # UPDATE
    m = re.match(r"update \w+ set v\s*=\s*(\d+) where k\s*=\s*(\d+)", stmt, re.I)
    if m:
        v, k = m.groups()
        return f"{prefix_str}-MATCH (n:{label} {{k: {k}}}) SET n.v = {v};"

    # BEGIN
    if stmt.lower() == "begin":
        return f"{prefix_str}-:begin"

    # COMMIT
    if stmt.lower() == "commit":
        return f"{prefix_str}-:commit"

    # 默认保留
    return f"{prefix_str}// [Unmapped] {stmt}"


def convert_folder(input_folder: str, output_folder: str):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for filename in os.listdir(input_folder):
        if filename.endswith(".txt"):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)

            with open(input_path, "r", encoding="utf-8") as infile:
                lines = infile.readlines()

            converted_lines = [sql_to_cypher(line) for line in lines]

            with open(output_path, "w", encoding="utf-8") as outfile:
                outfile.write("\n".join(converted_lines))

            print(f"Converted {filename} → {output_path}")


if __name__ == "__main__":
    input_folder = "./../t/pg"        
    output_folder = "./../t/neo4j"  
    convert_folder(input_folder, output_folder)
