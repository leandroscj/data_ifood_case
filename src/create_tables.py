import os
import json
import glob

def run_ddl():
    with open("param.json", "r", encoding="utf-8") as f:
            params = json.load(f)
    bucket = params.get("BUCKET")

    ddl_files = glob.glob(os.path.join("ddl","*.sql"))

    for filepath in ddl_files:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read().replace("ifood-case-406207085720", bucket)
            for stmt in content.split(";"):
                stmt = stmt.strip()
                if stmt:
                    print(f"Executing: {stmt}")
                    spark.sql(stmt)

if __name__ == "__main__":
    run_ddl()
