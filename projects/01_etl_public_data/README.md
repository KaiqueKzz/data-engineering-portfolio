# Projeto 01 — Pipeline ETL de Dados Climáticos (Open-Meteo)

## 📌 Objetivo
Construir um pipeline de Engenharia de Dados para **extração, transformação e carga (ETL)** de dados climáticos em tempo real, utilizando uma API pública, com foco em organização, reprodutibilidade e boas práticas.

Este projeto simula um cenário real de ingestão de dados externos para análises posteriores.

---

## 🏗 Arquitetura do Pipeline

API Open-Meteo  
→ Extract (Python + Requests)  
→ Transform (Pandas)  
→ Load (Parquet)  

Os dados tratados são armazenados em formato **Parquet**, seguindo boas práticas de engenharia de dados.

---

## 🛠 Stack Utilizada
- Python 3
- Requests (consumo de API)
- Pandas (transformações)
- PyArrow / Parquet (armazenamento colunar)
- Git / GitHub

---

## 📂 Estrutura do Projeto
