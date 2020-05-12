import datetime as dt
import numpy as np
import pandas as pd

import pandas.io.sql as sqlio
from blupy.connections.db_connections import query_in_db, connect as db_connections

data_hoje = (dt.date.today() + dt.timedelta(days=1)).strftime('%Y-%m-%d')

query = f'''
SELECT
    de.id AS id,
    de.client_id AS client_id,
    para.client_id AS client_id_dest,
    para.id AS id_dest,
    ct.gross_value,
    ct.created_at
FROM client_transactions AS ct
JOIN clients AS de ON ct.client_id = de.id
JOIN clients AS para ON ct.client_receiver_id = para.id
WHERE 
    de.department_id IN (9, 45, 6, 8, 36, 25) AND
    de.client_id != para.client_id AND
    ct.transaction_category_id = 33 AND
    ct.nature = 'outflow' AND
    de.distributor = FALSE AND
    para.distributor = TRUE AND
    ct.status = 'confirmed' AND
    ct.created_at >= '2019-01-01' AND
    ct.created_at <= '{data_hoje}'
ORDER BY
    ct.id,
    ct.happened_at;
'''
compras = query_in_db(query, 'pagnet_read_replica.json')

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

temp = compras.merge(enderecos, left_on='id', right_on='addressable_id', how='left')
result = temp.merge(clients, on='id', how='left')
result['tot_gross_value'] = -result['gross_value']
result['dia'] = result['created_at'].dt.date

compras_diarias_por_id = result.groupby(['id',
                                         'client_id',
                                         'dia',
                                         'department_id',
                                         'state',
                                         'cidade'], as_index=False)['tot_gross_value'].agg({'tot_compra': 'sum',
                                                                                            'media_compra': 'mean',
                                                                                            'mediana_compra': 'median',
                                                                                            'max_compra': 'max',
                                                                                            'min_compra': 'min',
                                                                                            'quantidade_de_compras': 'count'})
compras_diarias_por_id.to_csv('data/compras_diarias_por_id.csv', index=False)

recorte_cielo = compras_diarias_por_id[compras_diarias_por_id['dia'].apply(lambda d: d.year).isin([2019, 2020])]
recorte_cielo.to_csv('data/cielo_compra.csv', index=False)
