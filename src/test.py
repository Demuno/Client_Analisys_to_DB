import requests
import time


from src.main import df_treated

def consult_cep(cep):
    url = f"https://viacep.com.br/ws/{cep}/json/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao consultar CEP via ViaCEP: {e}")
    return None

df_treated['address'] = df_treated['address'].astype(str)
df_treated['logradouro'] = ''  # Criar uma nova coluna para armazenar o logradouro
df_treated['estado'] = ''

for index, row in df_treated.iterrows():
    cep = row['address'].replace('-', '').replace(' ', '').zfill(8)
    resultado = consult_cep(cep)

    if resultado:
        df_treated.at[index, 'logradouro'] = resultado['logradouro']
        df_treated.at[index, 'estado'] = resultado['estado']
    else:
        df_treated.at[index, 'logradouro'] = 'CEP não encontrado'
        df_treated.at[index, 'estado'] = 'CEP não encontrado'

