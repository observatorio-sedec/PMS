import ssl
import requests as rq
from datetime import datetime
import polars as pl
import concurrent.futures
import time

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
        try:
            dados_brutos_url = s.get(url, verify=True)
            if dados_brutos_url.status_code != 200:
                print(f"Aviso: Status {dados_brutos_url.status_code} para URL {url}")
                return None, None
            
            dados = dados_brutos_url.json()
            if len(dados) < 2:
                 return None, None
            return dados[0], dados[1]
        except Exception as e:
            print(f"Erro ao requisitar {url}: {e}")
            return None, None

def tratando_dados(dados_brutos_7167, dados_brutos_7168):
    dados_limpos_7167 = []
    dados_limpos_7168 = []
    variaveis = [dados_brutos_7167, dados_brutos_7168]

    for i in variaveis:
        if i is None: continue
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
                        id_local = iv['localidade']['id']
                        nome_local = iv['localidade']['nome']
                        dados_ano_producao = iv['serie'] 
                        
                        for ano, producao in dados_ano_producao.items():
                            partes = ano.split("/")
                            ano_num = int(partes[0][:4])
                            mes_num = partes[0][4:6]
                            producao = producao.replace('-', '0').replace('...', '0')
                            
                            row = {
                                'id': id_local,
                                'nome': nome_local,
                                'id_produto': id_produto,
                                'produto': nome_produto,
                                'unidade': unidade,
                                variavel: producao,
                                'ano': f'01/{mes_num}/{ano_num}'
                            }

                            if id_tabela == '7167':
                                dados_limpos_7167.append(row)
                            elif id_tabela == '7168':
                                dados_limpos_7168.append(row)
                                
    return dados_limpos_7167, dados_limpos_7168

def processar_url(url):
    try:
        raw_7167, raw_7168 = requisitando_dados(url)
        if raw_7167 is not None and raw_7168 is not None:
            return tratando_dados(raw_7167, raw_7168)
    except Exception as e:
        print(f"Erro processando {url}: {e}")
    return [], []

def executando_loop_datas():
    mes_atual = int(datetime.now().month)
    ano_atual = int(datetime.now().year)
    urls = []
    
    for ano in range(2018, ano_atual + 1):
        for mes in range(1, 13):
            if ano == ano_atual and mes > mes_atual:
                 break
            
            url = f'https://servicodados.ibge.gov.br/api/v3/agregados/5906/periodos/{ano}{mes:02d}/variaveis/7167|7168?localidades=N3[all]&classificacao=11046[all]'
            urls.append(url)
            
    print(f"Total URLs to process: {len(urls)}")
    
    lista_7167 = []
    lista_7168 = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(processar_url, u): u for u in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            l7167, l7168 = future.result()
            lista_7167.extend(l7167)
            lista_7168.extend(l7168)
            
    return lista_7167, lista_7168

def gerando_dataframe(lista_7167, lista_7168):
     schema_7167 = {
        'id': pl.Utf8, 'nome': pl.Utf8, 'id_produto': pl.Utf8, 'produto': pl.Utf8, 'unidade': pl.Utf8,
        'PMS - Número-índice (2022=100)': pl.Float64, 'ano': pl.Utf8
     }
     schema_7168 = {
        'id': pl.Utf8, 'nome': pl.Utf8, 'id_produto': pl.Utf8, 'produto': pl.Utf8, 'unidade': pl.Utf8,
        'PMS - Número-índice com ajuste sazonal (2022=100)': pl.Float64, 'ano': pl.Utf8
     }
     
     df1 = pl.DataFrame(lista_7167, schema=schema_7167) if lista_7167 else pl.DataFrame(schema=schema_7167)
     df2 = pl.DataFrame(lista_7168, schema=schema_7168) if lista_7168 else pl.DataFrame(schema=schema_7168)
     
     if df1.is_empty() or df2.is_empty():
         return pl.DataFrame()
     
     df_final = df1.join(df2, on=['id', 'nome', 'id_produto', 'produto', 'unidade', 'ano'], how='inner')
     df_final = df_final.with_columns(
        pl.col('ano').str.to_date('%d/%m/%Y')
    )
     df_final = df_final.rename({'nome': 'estado', 'produto': 'Indices'})
     
     return df_final

inicio = time.perf_counter()
dados_limpos_7167, dados_limpos_7168 = executando_loop_datas()
dataframe = gerando_dataframe(dados_limpos_7167, dados_limpos_7168)

# caminho = 'C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\PMS.xlsx'
# dataframe.write_excel(caminho)
print(dataframe)

fim = time.perf_counter()
print(f"Tempo de execução: {round(fim - inicio, 2)}")
if __name__ == '__main__':
    from sql import executar_sql
    executar_sql()