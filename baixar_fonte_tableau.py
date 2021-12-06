import tableauserverclient as TSC
import zipfile

SERVIDOR = 'servidor'
LOGIN = 'login'
SENHA = 'senha'
FONTE_TABLEAU = 'fonte'

print('Download ............')

tableau_auth = TSC.TableauAuth(LOGIN, SENHA)
opcoes = TSC.RequestOptions(pagesize=1000)  # altera o retorno padrao
server = TSC.Server(SERVIDOR, use_server_version=True)

with server.auth.sign_in(tableau_auth):
    all_datasources, pagination_item = server.datasources.get(opcoes)

    for datasource in all_datasources:
        if datasource.name == FONTE_TABLEAU:
            id_data = datasource.id

    caminho = server.datasources.download(id_data)

# EXTRAI ARQUIVO HYPER DO ZIP
print('Unzip ............')
with zipfile.ZipFile(caminho, "r") as zip_ref:
    arq = zip_ref.namelist()
    for a in arq:
        if a.endswith('.hyper'):
            saida_hyper = a
            zip_ref.extract(a)

print(f'Saida {saida_hyper}, ............')
