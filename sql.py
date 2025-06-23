from conexão import conexao
import psycopg2
from etl_pms import dataframe

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO pms, public')
    
    dados_pms = '''
    CREATE TABLE IF NOT EXISTS pms.indicadores_servicos (
    id INTEGER,
    estado VARCHAR(50) ,
    id_produto VARCHAR(50),
    indices VARCHAR(50),
    unidade VARCHAR(50),
    pms_numero_indice NUMERIC,
    data_indicador date,
    pms_numero_indice_ajustado NUMERIC);
    '''
    
    cur.execute(dados_pms)
    verificando_existencia_pms_dados = f'''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pms' AND table_type='BASE TABLE' AND table_name='indicadores_servicos';
    '''
    
    cur.execute(verificando_existencia_pms_dados)
    resultado_pms = cur.fetchone()
    if resultado_pms[0] == 1:
        dropando_tabela_dados = f'''
        TRUNCATE TABLE pms.indicadores_servicos;
        '''
        cur.execute(dropando_tabela_dados)
    
    inserindo_indicadores_servicos = '''
        INSERT INTO pms.indicadores_servicos (id, estado, id_produto, indices, unidade, 
            pms_numero_indice, data_indicador, pms_numero_indice_ajustado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in dataframe.iterrows():
            dados = (
            i['id'],
            i['estado'],
            i['id_produto'],
            i['Indices'],
            i['unidade'],
            i['PMS - Número-índice (2022=100)'],
            i['ano'],
            i['PMS - Número-índice com ajuste sazonal (2022=100)']
            )
            cur.execute(inserindo_indicadores_servicos, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados: {e}")

    conexao.commit()
    cur.close()