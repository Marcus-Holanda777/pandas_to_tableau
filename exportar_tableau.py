from tableauhyperapi import (HyperProcess, Connection, TableDefinition, SqlType,
                             Telemetry, Inserter, CreateMode, TableName)
from tableau_api_lib import TableauServerConnection
from datetime import date, datetime
import numpy as np


def retornaColTipos(frame):
    colunas = []

    datas = (date, datetime, np.datetime64)
    inteiros = (np.int0, np.int8, np.int16, np.int32, np.int64, int)
    texto = (str, np.object0)
    pontos = (np.double, float, np.float16, np.float32, np.float64)

    frame = frame.dropna()  # retirar colunas e linhas nulas

    for c in frame.columns:
        if isinstance(frame[c].head(1).values[0], datas):
            colunas.append(TableDefinition.Column(c, SqlType.date()))
        if isinstance(frame[c].head(1).values[0], inteiros):
            colunas.append(TableDefinition.Column(c, SqlType.int()))
        if isinstance(frame[c].head(1).values[0], texto):
            colunas.append(TableDefinition.Column(c, SqlType.text()))
        if isinstance(frame[c].head(1).values[0], pontos):
            colunas.append(TableDefinition.Column(c, SqlType.double()))

    return colunas


def exportarIterador(iterador, servidor, login, senha, to_saida, to_fonte):
    CONFIG = {
        'my_env': {
            'server': servidor,
            'api_version': '3.7',
            'username': login,
            'password': senha,
            'site_name': '',
            'site_url': ''
        }
    }
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'meuapp') as hyper:

        with Connection(endpoint=hyper.endpoint,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        database=to_saida) as connection:

            connection.catalog.create_schema('Extract')

            # pegar informacao das colunas
            frame_col = next(iterador)

            schema = TableDefinition(
                table_name=TableName('Extract', 'Extract'),
                columns=retornaColTipos(frame_col))

            connection.catalog.create_table(schema)

            with Inserter(connection, schema) as inserter:

                # inserido volumes
                for index, row in frame_col.iterrows():
                    inserter.add_row(row)

                for frame_in in iterador:
                    for index, row in frame_in.iterrows():
                        inserter.add_row(row)
                inserter.execute()

        print("A conexão com o arquivo Hyper está fechada.")

    conn = TableauServerConnection(config_json=CONFIG, env='my_env')
    conn.sign_in()

    response = conn.publish_data_source(datasource_file_path=to_saida,
                                        datasource_name=to_fonte,
                                        project_id='c2f823f5-790c-49bb-996f-a1e022125bc8')

    print(response.json())

    conn.sign_out()


def exportarFrame(frame_in, servidor, login, senha, to_saida, to_fonte):
    CONFIG = {
        'my_env': {
            'server': servidor,
            'api_version': '3.7',
            'username': login,
            'password': senha,
            'site_name': '',
            'site_url': ''
        }
    }
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'meuapp') as hyper:

        with Connection(endpoint=hyper.endpoint,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        database=to_saida) as connection:

            connection.catalog.create_schema('Extract')

            schema = TableDefinition(
                table_name=TableName('Extract', 'Extract'),
                columns=retornaColTipos(frame_in))

            connection.catalog.create_table(schema)

            with Inserter(connection, schema) as inserter:
                for index, row in frame_in.iterrows():
                    inserter.add_row(row)
                inserter.execute()

        print("A conexão com o arquivo Hyper está fechada.")

    conn = TableauServerConnection(config_json=CONFIG, env='my_env')
    conn.sign_in()

    response = conn.publish_data_source(datasource_file_path=to_saida,
                                        datasource_name=to_fonte,
                                        project_id='c2f823f5-790c-49bb-996f-a1e022125bc8')

    print(response.json())

    conn.sign_out()
