import os
import pandas as pd
from ctypes import *

class FlisSearch:
    def __init__(self, path_to_dll, path_to_fedlog, max_size=4096):
        self.path_to_dll = path_to_dll
        self.path_to_fedlog = path_to_fedlog
        self.max_size = max_size
        self.dll = CDLL(self.path_to_dll)
        self.path = c_char_p(bytes(self.path_to_fedlog, encoding='utf-8'))
        self.data = create_string_buffer(self.max_size)
        self.error = create_string_buffer(self.max_size)
        self.length = c_int(self.max_size)
        if self.dll.IMDConnectDLL(self.path):
            raise ValueError("Invalid FedLog Path or unable to connect to DLL")

    def _run_query(self, sql_query):
        self.data.value = b""
        self.dll.IMDSqlDLL(sql_query.encode('utf-8'), self.data, self.length)
        data_str = self.data.value.decode('utf-8').strip()
        if not data_str:
            return pd.DataFrame()
        data_lines = [line.strip('<>').strip() for line in data_str.split('\n')]
        data_split = [line.split('|') for line in data_lines]
        if len(data_split) < 2:
            return pd.DataFrame()
        columns = data_split[0]
        rows = data_split[1:]
        return pd.DataFrame(rows, columns=columns)

    def get_part_number_and_description(self, niin):
        sql_query = f"SELECT PART_NUMBER, ITEM_NAME FROM P_PART_PICK WHERE NIIN='{niin}'"
        df_parts = self._run_query(sql_query)
        if df_parts.empty:
            print('is empty')
            return {"part_number": "", "description": ""}
        distinct_part_numbers = df_parts["PART_NUMBER"].unique().tolist()
        flattened_part_numbers = ",".join(distinct_part_numbers)
        descriptions = df_parts["ITEM_NAME"].unique().tolist()
        description_str = ",".join(descriptions)
        return {"part_number": flattened_part_numbers, "description": description_str}

    def get_cage_codes_and_pricing(self, niin):
        sql_query_part = f"SELECT PART_NUMBER, ITEM_NAME, FSC, NIIN, CAGE_CODE FROM P_PART_PICK WHERE NIIN='{niin}'"
        df_part = self._run_query(sql_query_part)
        if df_part.empty:
            return pd.DataFrame()
        cage_codes = df_part["CAGE_CODE"].unique().tolist()
        if not cage_codes:
            return df_part
        df_cage_list = []
        for cage_code in cage_codes:
            sql_query_cage = f"SELECT CAGE_CODE, COMPANY, CAGE_STATUS FROM P_CAGE WHERE CAGE_CODE='{cage_code}'"
            df_cage = self._run_query(sql_query_cage)
            if not df_cage.empty:
                df_cage_list.append(df_cage)
        if df_cage_list:
            df_cage_all = pd.concat(df_cage_list, ignore_index=True)
        else:
            df_cage_all = pd.DataFrame(columns=["CAGE_CODE","COMPANY","CAGE_STATUS"])
        df_merged = pd.merge(df_part, df_cage_all, on="CAGE_CODE", how="left")
        sql_query_price = f"SELECT NIIN, UNIT_PRICE, EFFECTIVE_DATE FROM V_FLIS_MANAGEMENT WHERE NIIN='{niin}'"
        df_price = self._run_query(sql_query_price)
        if not df_price.empty:
            df_final = pd.merge(df_merged, df_price, on="NIIN", how="left")
        else:
            df_final = df_merged
        df_final["EFFECTIVE_DATE"] = pd.to_datetime(
                df_final["EFFECTIVE_DATE"], 
                format="%d-%b-%Y", 
                errors="coerce"
        )
        df_final = df_final.sort_values(by="EFFECTIVE_DATE", ascending=False)
        df_final.drop_duplicates(
                subset=["COMPANY"], 
                keep="first", 
                inplace=True
        )
        return df_final

    def close_connection(self):
        self.dll.IMDDisconnectDLL(self.path)
        print("DLL connection closed.")
