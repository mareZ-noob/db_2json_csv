import psycopg2
import json, codecs
import os, shutil
from decimal import Decimal
import itertools
import maskpass
import pandas as pd

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

HOST_NAME = input("host: ")
DB_NAME = input("dbname: ")
USER = input("user: ")
PASSWORD = maskpass.askpass(prompt="password: ", mask="*")
PORT_ID = int(input("port: "))

conn = cur = None
try: 
    conn = psycopg2.connect(
        host=HOST_NAME, 
        dbname=DB_NAME, 
        user=USER, 
        password=PASSWORD, 
        port=PORT_ID
    )
    cur = conn.cursor()

    table_script = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'public' ORDER BY table_name ASC"
    cur.execute(table_script)
    table = cur.fetchall()
    table = list(itertools.chain(*table))
    table_data = []
    for i in range(0, len(table)):
        table_data.append(table[i])
    
    current_path = os.getcwd()
    dir_name = "Json"
    folder_path = os.path.join(current_path, dir_name)
    if os.path.exists(dir_name):
        shutil.rmtree(folder_path)
    os.makedirs(dir_name)

    for i in range(0, len(table_data)):
        file_name = table_data[i] + ".json"
        file_path = os.path.join(folder_path, file_name)
        with codecs.open(filename=file_path, mode="w", encoding="utf-8") as ofs:
            select_script = f"SELECT * FROM {table_data[i]}"
            cur.execute(select_script)
            rows = cur.fetchall()

            column_script = "SELECT column_name FROM information_schema.columns WHERE table_name = "
            cur.execute(column_script + "'" + table_data[i] + "' " + "ORDER BY ordinal_position")
            columns = cur.fetchall()
            columns = list(itertools.chain(*columns))
            
            data = []
            for i in range(0, len(rows)):
                data_dict = {}
                for j in range(0, len(columns)):
                    data_dict[columns[j]] = rows[i][j]
                data.append(data_dict)

            json.dump(obj=data, fp=ofs, indent=4, ensure_ascii=False, cls=DecimalEncoder, default=str)
            conn.commit()

    # To csv
    csv_dir = "Csv"
    csv_path = os.path.join(current_path, csv_dir)
    if os.path.exists(csv_dir):
        shutil.rmtree(csv_path)
    os.makedirs(csv_dir)

    list_dirs = os.listdir(folder_path)
    for file in list_dirs:
        file_dir = os.path.join(folder_path, file)
        with codecs.open(filename=file_dir, mode="r", encoding="utf-8") as ifs:
            df = pd.read_json(ifs)
        csv_filepath = os.path.join(csv_path, file.split(".")[0] + ".csv")
        df.to_csv(path_or_buf=csv_filepath, encoding="utf-8", index=False)
except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()    