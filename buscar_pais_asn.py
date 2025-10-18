import pandas as pd
import requests
import time

# Nome do arquivo de entrada e saída
INPUT_FILE = 'circuits_2025-10-18_17-13-31.csv'
OUTPUT_FILE = 'circuits_ENRIQUECIDO.csv'

# Endereço da API de GeoIP
API_URL = "http://ip-api.com/batch"

# Taxa limite da API (aproximadamente 45 por minuto)
RATE_LIMIT_DELAY = 1  # 1 segundo entre as chamadas para garantir a não saturação.

# --- 1. Carregar e identificar IPs para enriquecer ---
try:
    df = pd.read_csv(INPUT_FILE)
except FileNotFoundError:
    print(f"Erro: Arquivo '{INPUT_FILE}' não encontrado.")
    exit()

# Identifica os IPs que precisam de GeoIP (onde country ou asn é 'UNKNOWN')
ips_para_consulta = df[
    (df['country'] == 'UNKNOWN') | (df['asn'] == 'UNKNOWN')
]['ip'].unique()

print(f"Total de IPs únicos a serem consultados: {len(ips_para_consulta)}")
print("Iniciando consulta na API (pode levar alguns minutos devido ao limite de requisições)...")

# --- 2. Consultar a API em Lotes ---
# Dicionário para armazenar o resultado da consulta (cache)
ip_cache = {}
batch_size = 100 
successful_queries = 0

for i in range(0, len(ips_para_consulta), batch_size):
    ip_batch = list(ips_para_consulta[i:i + batch_size])
    
    # Adiciona um atraso para respeitar o limite da API (apenas para lotes subsequentes)
    if i > 0:
        time.sleep(RATE_LIMIT_DELAY * batch_size) 

    try:
        # Consulta a API em lote (JSON com IPs)
        response = requests.post(API_URL, json=ip_batch, timeout=20)
        
        if response.status_code == 200:
            results = response.json()
            successful_queries += len(results)
            
            for result in results:
                ip = result.get('query')
                
                # Mapeia os dados relevantes para o cache
                if result.get('status') == 'success':
                    country = result.get('country')
                    asn = result.get('as') # 'as' é o campo para ASN na ip-api
                    
                    # Limpeza e padronização dos valores
                    country = country if country else 'UNKNOWN'
                    asn = asn if asn else 'UNKNOWN'
                    
                    ip_cache[ip] = {'country': country, 'asn': asn}
                else:
                     # Se a consulta falhar para o IP, marca como UNKNOWN
                    ip_cache[ip] = {'country': 'UNKNOWN', 'asn': 'UNKNOWN'}

        else:
            print(f"Erro na requisição em lote (Status Code: {response.status_code}).")
            break
            
    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão: {e}. Parando a consulta.")
        break
        
print(f"Consulta finalizada. IPs consultados com sucesso: {successful_queries}")

# --- 3. Aplicar os dados enriquecidos de volta ao DataFrame ---

# Define as funções de mapeamento
def map_country(row):
    ip = row['ip']
    if row['country'] == 'UNKNOWN' and ip in ip_cache:
        return ip_cache[ip]['country']
    return row['country']

def map_asn(row):
    ip = row['ip']
    if row['asn'] == 'UNKNOWN' and ip in ip_cache:
        return ip_cache[ip]['asn']
    return row['asn']

# Aplica as funções para atualizar as colunas
df['country'] = df.apply(map_country, axis=1)
df['asn'] = df.apply(map_asn, axis=1)

# --- 4. Salvar o novo DataFrame ---
df.to_csv(OUTPUT_FILE, index=False)
print(f"\nSucesso! O novo arquivo com dados enriquecidos foi salvo como: {OUTPUT_FILE}")

# Verifica quantos UNKNOWN foram resolvidos
unknown_after = (df['country'] == 'UNKNOWN').sum()
print(f"Total de linhas com 'UNKNOWN' no país após enriquecimento: {unknown_after}")

# Próximo passo: Rerun a análise com o arquivo '{OUTPUT_FILE}'