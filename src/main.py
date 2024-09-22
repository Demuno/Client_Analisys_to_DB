import os
import pandas as pd
import requests
import uuid
import re

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv

def generate_v4x_uuid():
    return str(uuid.uuid4())

load_dotenv()

API_KEY = os.getenv("API_KEY")

engine = create_engine(API_KEY, echo=True)

df = pd.read_excel('Clinica Bs/Clientes pilary.xlsx')
df.columns = df.columns.str.replace(' ', '')

df_treated = df[['Telefone', 'Email', 'Nome', 'CPF', 'Cep', 'Número']]
df_treated = df_treated.rename(columns={
    'Telefone': 'phone',
    'Email': 'email',
    'Nome': 'name',
    'CPF': 'document',
    'Cep': 'zip_code',
    'Número': 'number',
})

def consult_cep(cep):
    url = f"https://viacep.com.br/ws/{cep}/json/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao consultar CEP via ViaCEP: {e}")
    return None

df_treated['zip_code'] = df_treated['zip_code'].astype(str)
df_treated['logradouro'] = ''
df_treated['estado'] = ''

for index, row in df_treated.iterrows():
    cep = row['zip_code'].replace('-', '').replace(' ', '').zfill(8)
    resultado = consult_cep(cep)

    if resultado:
        df_treated.at[index, 'logradouro'] = resultado['logradouro']
        df_treated.at[index, 'estado'] = resultado['estado']
    else:
        df_treated.at[index, 'logradouro'] = 'CEP não encontrado'
        df_treated.at[index, 'estado'] = 'CEP não encontrado'

df_treated = df_treated.rename(columns={
    'logradouro': 'street',
    'estado': 'city'
})

df_treated_address = df_treated.copy()
df_treated_address['id'] = [generate_v4x_uuid() for _ in range(len(df_treated_address))]
df_treated_address['created_at'] = datetime.now()

df_treated_address['number'] = df_treated_address['number'].astype(str) \
    .str.replace(r'[a-zA-Z]', '', regex=True)

df_treated_address['number'] = pd.to_numeric(df_treated_address['number'], errors='coerce')

df_treated_address = df_treated_address[['id', 'street', 'number', 'zip_code', 'created_at', 'city']]

df_treated_address.to_sql('address', con=engine, if_exists='append', index=False)

df_treated_clients = df_treated.copy()
df_treated_clients['id'] = [generate_v4x_uuid() for _ in range(len(df))]
df_treated_clients['created_at'] = datetime.now()
df_treated_clients['observations'] = 'Sem Observações'

df_treated_clients['document'] = df_treated_clients['document'].astype(str)
df_treated_clients['document'] = df_treated_clients['document'].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', x))
df_treated_clients['document'] = df_treated_clients['document'].str.replace(' ', '', regex=True)

df_treated_address.rename(columns={'id': 'address_id'}, inplace=True)

df_treated_clients['address_id'] = df_treated_address['address_id'].values

df_treated_clients = df_treated_clients[['id', 'phone', 'email', 'name', 'created_at', 'document', 'address_id', 'observations']]

df_treated_clients.to_sql('clients', con=engine, if_exists='append', index=False)

print(df_treated_clients)