import datetime as dt
import numpy as np
import pandas as pd

import pandas.io.sql as sqlio
from blupy.connections.db_connections import query_in_db, connect as db_connections

data_hoje = (dt.date.today() + dt.timedelta(days=1)).strftime("%Y-%m-%d")

query = f'''
SELECT
    lj.id AS id,
    mt.id AS client_id,
    ct.happened_at,
    ct.gross_value,
    lj.name AS loja,
    mt.name AS matriz,
    ct.installment,
    ct.installments
FROM clients AS lj
JOIN clients AS mt ON lj.client_id = mt.id
JOIN client_transactions AS ct ON lj.id = ct.client_id
WHERE 
    lj.department_id IN (9, 45, 6, 8, 36, 25) AND
    lj.distributor = FALSE AND
    ct.transaction_category_id IN (92, 93) AND
    ct.nature = 'inflow' AND
    ct.status = 'confirmed' AND
    ct.happened_at >= '2019-01-01' AND
    ct.happened_at <= '{data_hoje}'
ORDER BY
    ct.id,
    ct.happened_at;
'''
vendas = query_in_db(query, 'pagnet_read_replica.json')

# query = 'SELECT * FROM fact_ganhos'
# ganhos = query_in_db(query, 'dw_redshift.json')

query = '''
SELECT
    id,
    department_id
FROM clients;
'''
clients = query_in_db(query, 'pagnet_read_replica.json')

query = '''
SELECT
    addressable_id,
    state,
    city
FROM addresses
WHERE
    addressable_type = 'Client' AND
    main = 'true';
'''
enderecos = query_in_db(query, 'pagnet_read_replica.json')
enderecos['state'] = enderecos['state'].str.strip().str.lower()
enderecos['city'] = enderecos['city'].str.strip().str.lower()
enderecos.rename({'city': 'cidade'}, axis=1, inplace=True)

temp = vendas.merge(enderecos, left_on='id', right_on='addressable_id', how='left')
result = temp.merge(clients, on='id', how='left')
result = result[result['installment'] == 1]
result['tot_gross_value'] = result['gross_value'] * result['installments']
result['dia'] = result['happened_at'].dt.date

vendas_diarias_por_id = result.groupby(['id',
                                        'client_id',
                                        'dia',
                                        'department_id',
                                        'state',
                                        'cidade'], as_index=False)['tot_gross_value'].agg({'tot_venda': 'sum',
                                                                                           'media_venda': 'mean',
                                                                                           'mediana_venda': 'median',
                                                                                           'max_venda': 'max',
                                                                                           'min_venda': 'min',
                                                                                           'quantidade_de_vendas': 'count'})
vendas_diarias_por_id.to_csv('data/vendas_diarias_por_id.csv', index=False)

recorte_cielo = vendas_diarias_por_id[vendas_diarias_por_id['dia'].apply(lambda d: d.year).isin([2019, 2020])]
recorte_cielo.to_csv('data/cielo.csv', index=False)
