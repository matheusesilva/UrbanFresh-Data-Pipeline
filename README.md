# UrbanFresh Data Pipeline

Este projeto implementa um pipeline de ETL (Extract, Transform, Load) para processar e analisar dados de pedidos da UrbanFresh, provenientes de diferentes canais (mobile, online e loja física). O objetivo é consolidar, limpar e transformar os dados brutos em relatórios e arquivos processados para facilitar análises e tomadas de decisão.

## Estrutura do Projeto
- `main.py`: Script principal para execução do pipeline.
- `src/etl_pipeline.py`: Lógica do pipeline ETL, incluindo extração, transformação, limpeza e geração de relatórios.
- `data/raw/`: Dados brutos de pedidos.
- `data/processed/orders/`: Dados processados e relatórios gerados.
- `logs/`: Arquivos de log do pipeline.

## Funcionalidades
- Extração de dados de múltiplos arquivos CSV.
- Limpeza e padronização de campos (ex: customer_id, price, datas).
- Remoção de registros de teste e duplicados.
- Geração de relatórios resumidos por região, ano e mês.
- Exportação dos dados processados para CSV.

## Requisitos
- Python 3.8+
- PySpark

## Execução
1. Instale as dependências.
2. Execute o `main.py` para iniciar o pipeline (--log_level define o nível de log do arquivo salvo em /logs. Ex: DEBUG, INFO, WARNING, ERROR e CRITICAL)
