from tableauhyperapi.hyperprocess import Telemetry
from tableauhyperapi.hyperprocess import HyperProcess
from tableauhyperapi import Connection
import pandas as pd

arquivo_hyper = 'myhyper.hyper'

print('Carregando os dados ....')
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(hyper.endpoint, database=arquivo_hyper) as connection:

        for shmenas in connection.catalog.get_schema_names():
            for table in connection.catalog.get_table_names(shmenas):
                defin_table = connection.catalog.get_table_definition(table)
                columns_all = []
                for column in defin_table.columns:
                    columns_all.append(str(column.name).strip('"'))

                row = connection.execute_list_query(
                    f'select * from {table} ')


print('Transformando os dados ....')
df_frame = pd.DataFrame(row, columns=columns_all)  # data frame
