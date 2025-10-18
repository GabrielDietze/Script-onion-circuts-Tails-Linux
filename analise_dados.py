import pandas as pd
import numpy as np

# A lista de países da aliança Fourteen Eyes para o TCC
fourteen_eyes = ['Germany', 'Belgium', 'Canada', 'Denmark', 'Spain', 'United States', 'France', 'Italy', 'Norway', 'New Zealand', 'Netherlands', 'United Kingdom', 'Sweden', 'Australia']
fourteen_eyes_lower = [c.lower() for c in fourteen_eyes]

# Load the CSV file (substitua pelo seu arquivo corrigido)
df = pd.read_csv('circuits_enriquecido.csv')

# --- 0. Data Preparation and Cleaning ---
# Filtra linhas com dados ausentes ('UNKNOWN') nos campos críticos
df_clean = df[(df['country'] != 'UNKNOWN') & (df['asn'] != 'UNKNOWN') & (df['role'].isin(['guard', 'middle', 'exit']))].copy()
total_clean_nodes = len(df_clean)

output = []
output.append("## ANÁLISE QUANTITATIVA DA REDE TOR (TCC)")
output.append(f"\n[Diagnóstico de Dados: {total_clean_nodes} nós válidos para análise após limpeza.]")

# --- 1. Centralização da Rede por Provedores (ASN) ---

output.append("\n\n## 1. Centralização da Rede por Provedores (ASN)")

# 1.1 Dominância de Provedores
asn_counts = df_clean['asn'].value_counts()
asn_percentage = (asn_counts / total_clean_nodes) * 100
asn_cumulative = asn_percentage.cumsum()

def get_concentration_percent(cumulative_series, rank):
    if len(cumulative_series) == 0:
        return 0.0
    if len(cumulative_series) >= rank:
        return cumulative_series.iloc[rank - 1]
    else:
        return cumulative_series.iloc[-1]

top_5_percent = get_concentration_percent(asn_cumulative, 5)
top_10_percent = get_concentration_percent(asn_cumulative, 10)

output.append("\n1.1 Dominância de Provedores (Métricas de Concentração):")
output.append(f"- Os 5 ASNs mais comuns controlam: {top_5_percent:.2f}% da rede observada.")
output.append(f"- Os 10 ASNs mais comuns controlam: {top_10_percent:.2f}% da rede observada.")

# 1.2 Distribuição de ASNs por Função (Dados para Gráfico: grafico_asn_por_funcao.png)
asn_role_distribution = df_clean.groupby(['asn', 'role']).size().unstack(fill_value=0)
top_20_asn = df_clean['asn'].value_counts().head(20).index
asn_role_top_20 = asn_role_distribution.loc[top_20_asn].fillna(0).astype(int)
asn_role_top_20.to_csv('data_asn_por_funcao.csv', index=True)
output.append("\n1.2. Dados para o gráfico de ASN por Função salvos em: data_asn_por_funcao.csv")


# --- 2. Análise do Risco de Correlação de Tráfego ---

output.append("\n\n## 2. Análise do Risco de Correlação de Tráfego")

# Identifica circuitos completos (Guard, Middle, Exit)
circuit_groups = df_clean.groupby('circuit_id')
complete_circuit_ids = circuit_groups.filter(lambda x: len(x) == 3 and x['role'].nunique() == 3)['circuit_id'].unique()
df_circuits = df_clean[df_clean['circuit_id'].isin(complete_circuit_ids)].copy()
circuits_pivot = df_circuits.pivot(index='circuit_id', columns='role', values=['country', 'asn']).reset_index()
circuits_pivot.columns = [f"{col[0]}_{col[1]}" for col in circuits_pivot.columns.values]
circuits_pivot.rename(columns={
    'country_guard': 'guard_country', 'country_middle': 'middle_country', 'country_exit': 'exit_country',
    'asn_guard': 'guard_asn', 'asn_middle': 'middle_asn', 'asn_exit': 'exit_asn'
}, inplace=True)
total_circuits = len(circuits_pivot)
output.append(f"2.0. Total de circuitos completos analisados: {total_circuits}")


if total_circuits > 0:

    # 2.1 Risco Geográfico Direto
    same_country_risk = (circuits_pivot['guard_country'] == circuits_pivot['exit_country']).sum()
    same_asn_risk = (circuits_pivot['guard_asn'] == circuits_pivot['exit_asn']).sum()
    percent_same_country = (same_country_risk / total_circuits) * 100
    percent_same_asn = (same_asn_risk / total_circuits) * 100

    output.append("\n2.1 Risco Geográfico Direto:")
    output.append(f"- Risco por País (Guard e Exit no MESMO País): {percent_same_country:.2f}% ({same_country_risk} de {total_circuits})")
    output.append(f"- Risco por ASN (Guard e Exit no MESMO ASN): {percent_same_asn:.2f}% ({same_asn_risk} de {total_circuits})")

    # 2.2 Risco de Colaboração Interestatal (Fourteen Eyes)
    def is_in_fourteen_eyes(country):
        return country.lower() in fourteen_eyes_lower

    guard_in_14eyes = circuits_pivot['guard_country'].apply(is_in_fourteen_eyes)
    exit_in_14eyes = circuits_pivot['exit_country'].apply(is_in_fourteen_eyes)
    jurisdictional_risk_circuits = circuits_pivot[
        (circuits_pivot['guard_country'] != circuits_pivot['exit_country']) &
        (guard_in_14eyes) &
        (exit_in_14eyes)
    ]
    percent_jurisdictional_risk = (len(jurisdictional_risk_circuits) / total_circuits) * 100

    output.append("\n2.2 Risco de Colaboração Interestatal (Fourteen Eyes):")
    output.append(f"- Porcentagem de circuitos (Guard e Exit) em países DIFERENTES, mas AMBOS no grupo Fourteen Eyes: {percent_jurisdictional_risk:.2f}% ({len(jurisdictional_risk_circuits)} de {total_circuits})")

    # 2.3 Padrões de Construção de Circuito (Top 10 Trilhas de Países)
    def get_circuit_path(group):
        role_order = {'guard': 0, 'middle': 1, 'exit': 2}
        sorted_group = group.sort_values(by='role', key=lambda x: x.map(role_order))
        return ' -> '.join(sorted_group['country'])

    circuit_paths = df_circuits.groupby('circuit_id').apply(get_circuit_path)
    top_10_paths = circuit_paths.value_counts().head(10)

    output.append("\n2.3 Padrões de Construção de Circuito (Top 10 Trilhas de Países):")
    output.append(top_10_paths.to_markdown(numalign="left", stralign="left"))

else:
    output.append("\nNão foi possível realizar a análise de correlação: 0 circuitos completos encontrados após a limpeza.")


# --- 3. Distribuição Geográfica por Função ---

output.append("\n\n## 3. Distribuição Geográfica por Função")

# 3.1 Gráfico Geral de Países (Dados para Gráfico: grafico_paises_geral.png)
country_counts = df_clean['country'].value_counts().head(15)
country_counts.name = 'Contagem_Nós'
country_counts.to_csv('data_paises_geral.csv', header=True)
output.append("3.1. Dados para o gráfico geral de Países salvos em: data_paises_geral.csv")


# 3.2 Gráfico Países por Função (Dados para Gráfico: grafico_paises_por_funcao.png)
country_role_distribution = df_clean.groupby(['country', 'role']).size().unstack(fill_value=0)
top_20_countries = df_clean['country'].value_counts().head(20).index
country_role_top_20 = country_role_distribution.loc[top_20_countries].fillna(0).astype(int)
country_role_top_20.to_csv('data_paises_por_funcao.csv', index=True)
output.append("3.2. Dados para o gráfico de Países por Função salvos em: data_paises_por_funcao.csv")

print('\n'.join(output))