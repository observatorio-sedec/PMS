import ssl
import pandas as pd
import requests as rq
from datetime import datetime

class TLSAdapter(rq.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)


def requisitando_dados(url):
    with rq.session() as s:
        s.mount("https://", TLSAdapter())
        dados_brutos_url = s.get(url, verify=True)
    
    if dados_brutos_url.status_code != 200:
        raise Exception(f"A solicitação à url falhou com o código de status: {dados_brutos_url.status_code}")

    try:
        dados_brutos = dados_brutos_url.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da url: {str(e)}")

    if len(dados_brutos) < 2:
        dados_brutos_7167 = None
        dados_brutos_7168 = None
        return dados_brutos_7167, dados_brutos_7168
    
    if dados_brutos_url.status_code == 500:
        raise Exception(f"Os dados passou de 100.0000 por isso o codigo de: {dados_brutos_url.status_code}")

    dados_brutos_7167= dados_brutos[0]
    dados_brutos_7168 = dados_brutos[1]
    return dados_brutos_7167, dados_brutos_7168

def tratando_dados(dados_brutos_7167, dados_brutos_7168):
    dados_limpos_7167= []
    dados_limpos_7168 = []

    variaveis = [dados_brutos_7167, dados_brutos_7168]

    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']

        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']
            
            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():
    
                    for iv in dados_producao:
                        id = iv['localidade']['id']
                        nome = iv['localidade']['nome']
                        dados_ano_producao = iv['serie'] 
                        
                        for ano, producao in dados_ano_producao.items():
                            partes = ano.split("/")
                            ano = int(partes[0][:4])
                            mes = (partes[0][4:6])
                            producao = producao.replace('-', '0').replace('...', '0')
                            
                            dict = {

                                'id': id,
                                'nome': nome,
                                'id_produto': id_produto,
                                'produto': nome_produto,
                                'unidade': unidade,
                                variavel: producao,
                                'ano': f'01/{mes}/{ano}'   
                            }
                           
                            if id_tabela == '7167':
                                dados_limpos_7167.append(dict)
                            elif id_tabela == '7168':
                                dados_limpos_7168.append(dict)


    return dados_limpos_7167, dados_limpos_7168

def gerando_dataframe(dados_limpos_7167, dados_limpos_dados_brutos_7168):
    df7167= pd.DataFrame(dados_limpos_7167)
    df7168 = pd.DataFrame(dados_limpos_dados_brutos_7168)
    dataframe = pd.merge(df7167, df7168, on=['id', 'nome', 'id_produto', 'produto', 'unidade', 'ano'], how='inner')
    dataframe['PMS - Número-índice (2022=100)'] = dataframe['PMS - Número-índice (2022=100)'].astype(float)
    dataframe['PMS - Número-índice com ajuste sazonal (2022=100)'] = dataframe['PMS - Número-índice com ajuste sazonal (2022=100)'].astype(float)
    dataframe = dataframe.rename(columns={'nome': 'estado','produto': 'Indices'})

    return dataframe

def executando_loop_datas():
    mes_atual = int(datetime.now().month)
    ano_atual = int(datetime.now().year)
    lista_dados_7167= [] 
    lista_dados_7168 = []
    for ano in range(2018, ano_atual+1):
        for mes in range(1, 13):

            if mes == 10 or mes == 11 or mes == 12:
                url = f'https://servicodados.ibge.gov.br/api/v3/agregados/5906/periodos/{ano}{mes}/variaveis/7167|7168?localidades=N3[all]&classificacao=11046[all]'     
            else:
                url = f'https://servicodados.ibge.gov.br/api/v3/agregados/5906/periodos/{ano}0{mes}/variaveis/7167|7168?localidades=N3[all]&classificacao=11046[all]'   
            variavel7167, variavel_7168= requisitando_dados(url)
            if variavel7167 == None and variavel_7168 == None:
                break
            if len(variavel7167) == 0 and len(variavel_7168) == 0 :
                lista_dados_7167, lista_dados_7168= tratando_dados(variavel7167, variavel_7168)

            else:
                novos_dados_7167, novos_dados_7168= tratando_dados(variavel7167, variavel_7168)
                lista_dados_7167.extend(novos_dados_7167)
                lista_dados_7168.extend(novos_dados_7168)

    return  lista_dados_7167,lista_dados_7168

dados_limpos_7167, dados_limpos_dados_brutos_7168= executando_loop_datas()
dataframe = gerando_dataframe(dados_limpos_7167, dados_limpos_dados_brutos_7168)
dataframe.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PMS.xlsx', index=False)
print(dataframe)
if __name__ == '__main__':
    from sql import executar_sql
    executar_sql()