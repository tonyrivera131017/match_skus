import pandas as pd
import psycopg2
import openpyxl

# Função para estabelecer uma conexão com o banco de dados PostgreSQL
def conecta_db():
    con = psycopg2.connect(host='prod-karhub-core.cjyc7ghylv0m.us-east-1.rds.amazonaws.com',
                           database='karhub',
                           user='tony.rivera@karhub.com.br',
                           password='u9yKxkELt4BwJpX8CPHs3nzbDS!gY76W')
    return con

# Função para consultar o banco de dados com uma consulta SQL e retornar um DataFrame do pandas
def consultar_db(sql, params=None, limpar=False):
    con = conecta_db()
    con.cursor().execute('SET statement_timeout TO 0;')
    if params is not None:
        data = pd.io.sql.read_sql_query(sql=sql, con=con, params=params, dtype=str)
    else:
        data = pd.io.sql.read_sql_query(sql=sql, con=con, dtype=str)
    if limpar:
        return data.replace('None', '', regex=False).replace('NaT', '', regex=False).fillna('')
    else:
        return data
def rmv_char(text):
    list_char = [" ", ",", "<", ".", ">", ";", ":", "^", "~", "´", "`", "]", "[", "{", "}", "+", "-", "~*", "/", "\\",
                 "|", ")", "(", "&", "¨", "%", "$", "#", "@", "!", "'", "_", "=", '""']
    for i in list_char:
        text = text.replace(i,'')
    return text
def buscar_elemento(elemento, coluna2):
    if elemento in coluna2:
        return elemento
    else:
        return ''

#df_original = pd.read_excel('./-- estoque 27-10-2023.xls')

df = pd.read_excel('./compel SBI09112023_071732554.xls')
coluna_pn = 'Codigo'
coluna_marca = 'Fabricante'
#Limpando caracteres especiais da coluna que representa o partnumber
print(df[coluna_pn])
df['chave'] = df[coluna_marca] + df[coluna_pn]
df['chave'] = df['chave'].apply(rmv_char)
df['chave'] = df['chave'].apply(lambda x: x.lower())
print(df[coluna_pn])
#df[coluna_marca] = df[coluna_marca].apply(rmv_char)
#df[coluna_marca] = df[coluna_marca].apply(lambda x: x.lower())


sql_brand = 'select alias,brand_id from  brand_alias ba'
brand_alias = consultar_db(sql_brand,limpar=True)
#print(brand_alias['alias'])
brand_alias['alias'] = brand_alias['alias'].apply(rmv_char)
brand_alias['alias'] = brand_alias['alias'].apply(lambda x: x.lower())
#print(brand_alias['alias'])

sql_sku = 'select sku_id,alias,brand_id from sku_alias sa '
sku_alias = consultar_db(sql_sku,limpar=True)
sku_alias['alias'] = sku_alias['alias'].apply(rmv_char)
sku_alias['alias'] = sku_alias['alias'].apply(lambda x: x.lower())

dados = []

# Itere sobre os grupos de 'sku_id' em 'sku_alias'
for sku_id, dados_sku in sku_alias.groupby('sku_id'):
    # Combine os DataFrames com base em 'brand_id'
    resultado = pd.merge(dados_sku, brand_alias, on='brand_id')
    # Adicione o resultado à lista
    dados.append(resultado)

# Concatene todos os DataFrames da lista em um único DataFrame
dados_combinados = pd.concat(dados, ignore_index=True)
dados_combinados['chave'] = dados_combinados['alias_x'] + dados_combinados['alias_y']
#dados_combinados.

df = df.merge(dados_combinados[['chave','sku_id']], on='chave', how='left')
df.to_excel('resultado.xlsx', index=False)




