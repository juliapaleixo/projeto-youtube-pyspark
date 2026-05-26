# 📊 Análise de Dados do YouTube com PySpark

Pipeline completo de engenharia e análise de dados sobre vídeos do YouTube, construído com **Apache PySpark**. O projeto cobre todas as etapas de um fluxo de dados real: ingestão, limpeza, enriquecimento, agregações estatísticas, preparação para Machine Learning e otimização de joins.

---

## 🗂️ Estrutura do Projeto

```
projeto-youtube-pyspark/
│
├── notebooks/
│   ├── 01-leitura-escrita.ipynb     # Ingestão e persistência de dados
│   ├── 02-tratamento.ipynb          # Limpeza e transformação
│   ├── 03-preparacao.ipynb          # Feature engineering e ML
│   ├── 04-agregacao.ipynb           # Agregações e análise estatística
│   └── 05-otimizacao.ipynb          # Otimização de joins
│
├── data/
│   ├── raw/                         # Arquivos de entrada (.csv)
│   │   ├── videos-stats.csv
│   │   ├── comments.csv
│   │   └── USvideos.csv
│   └── processed/                   # Arquivos gerados pelo pipeline (.parquet)
│       ├── videos-parquet/
│       ├── videos-tratados-parquet/
│       ├── videos-comments-tratados-parquet/
│       ├── videos-preparados-parquet/
│       └── join-videos-comments-otimizado/
│
└── README.md
```

---

## 📁 Datasets

Os dados utilizados são de domínio público (licença **CC0**) e podem ser obtidos no Kaggle.

| Arquivo | Descrição | Registros |
|---|---|---|
| `videos-stats.csv` | Metadados de vídeos do YouTube (título, keyword, likes, views, comentários) | ~1.900 |
| `comments.csv` | Comentários associados aos vídeos com análise de sentimento | ~18.000 |
| `USvideos.csv` | Top trending videos dos EUA com dados históricos | ~40.000 |

**Período coberto:** 2007 – 2022  
**Keywords disponíveis:** animals, apple, asmr, bed, biology, business, chess, cnn, computer science, crypto, cubes, data science, education, finance, food, gaming, google, how-to, interview, marvel, music, mrbeast, mukbang, news, nintendo, physics, reaction, sat, sports, tech, tutorial, entre outras (41 categorias no total).

---

## 📓 Notebooks

### 01 — Leitura e Escrita
**Arquivo:** `notebooks/01-leitura-escrita.ipynb`

Introdução à leitura e persistência de dados com PySpark.

- Leitura de CSV com e sem inferência de esquema
- Comparação de schemas (`StringType` vs tipos inferidos)
- Persistência no formato **Parquet** com cabeçalho
- Registro de tabelas no **Spark Catalog**
- Consultas via **Spark SQL**

---

### 02 — Tratamento de Dados
**Arquivo:** `notebooks/02-tratamento.ipynb`

Limpeza, validação e integração dos datasets.

- Substituição de valores nulos por `0` em campos numéricos
- Remoção de registros com `Video ID` nulo ou duplicado
- Conversão de tipos: `float → int`, `string → date`
- Criação de campos derivados: `Interaction` (Likes + Comments + Views), `Year`
- Join entre `df_video` e `df_comentario` pela chave `Video ID`
- Join com dataset de trending videos (`USvideos.csv`) pelo campo `Title`
- Verificação de qualidade: contagem de nulos por coluna

---

### 03 — Preparação para Machine Learning
**Arquivo:** `notebooks/03-preparacao.ipynb`

Feature engineering e modelo preditivo.

| Transformação | Técnica PySpark |
|---|---|
| Extração do mês | `F.month()` |
| Encoding de categorias | `StringIndexer` (Keyword → Keyword Index) |
| Vetorização | `VectorAssembler` (5 features) |
| Normalização | `Normalizer` (norma L2) |
| Redução de dimensionalidade | `PCA` (5 → 1 componente, **90,1% da variância**) |
| Modelo preditivo | `LinearRegression` (estimativa de Comments) |

**Split:** 80% treino / 20% teste (`seed=42`)

**Métricas do modelo:**
| Métrica | Valor |
|---|---|
| RMSE | 43.345 |
| MAE | 11.982 |
| R² | 0,0083 |

> O R² baixo é esperado: a relação entre Likes/Views e Comments é não-linear. Modelos como Random Forest ou Gradient Boosting seriam mais adequados para próximas iterações.

---

### 04 — Agregações e Análise Estatística
**Arquivo:** `notebooks/04-agregacao.ipynb`

Extração de insights via `groupBy`, `agg` e Window Functions.

- Contagem de registros por keyword
- Média e variância de Views por keyword
- Ranking de interações máximas (`Rank Interactions`)
- Min/max de Views sem casas decimais
- Range de datas de publicação por keyword
- Detecção de títulos duplicados (15 encontrados)
- Distribuição temporal: registros por ano e por ano+mês
- **Média acumulativa de Likes** por keyword ao longo dos anos (Window Function com `rowsBetween`)

**Destaques:**
- `animals` lidera em interações máximas: **1,59 bilhão**
- `mrbeast` tem a maior consistência de views (~66M de média)
- Maior concentração de publicações: **agosto de 2022**

---

### 05 — Otimização de Joins
**Arquivo:** `notebooks/05-otimizacao.ipynb`

Três abordagens progressivas para o mesmo join, com análise via `explain()`.

#### 🔵 Abordagem 1 — Baseline
Leitura simples + `createOrReplaceTempView` + `spark.sql`. Sem controle de particionamento (1 partição por DataFrame).

#### 🟡 Abordagem 2 — repartition + coalesce
- `repartition(8, 'Video ID')`: redistribui dados pela chave de join (shuffle completo, aumenta paralelismo)
- `coalesce(4)`: reduz partições no resultado **sem shuffle** (mais eficiente para escrita)

#### 🟢 Abordagem 3 — Join Otimizado (8 técnicas)
| # | Técnica | Benefício |
|---|---|---|
| 1-2 | **Column pruning** | Seleciona apenas colunas necessárias antes do join |
| 3 | **Predicate pushdown** | Filtra nulos antes do join |
| 4 | **Co-particionamento** | `repartition(8, Video ID)` em ambos os lados |
| 5 | **Cache** | Persiste DataFrames na memória |
| 6 | **TempViews otimizadas** | Views a partir dos DFs já processados |
| 7 | **BROADCAST hint** | `/*+ BROADCAST(v) */` converte Sort Merge → Broadcast Hash Join |
| 8 | **coalesce antes de salvar** | Consolida partições sem shuffle extra |

---

## 🛠️ Tecnologias

| Tecnologia | Versão |
|---|---|
| Python | 3.10+ |
| Apache Spark / PySpark | 4.x |
| Jupyter Notebook | — |
| Parquet (Snappy) | — |

---

## ▶️ Como executar

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/projeto-youtube-pyspark.git
cd projeto-youtube-pyspark
```

### 2. Instale as dependências
```bash
pip install pyspark jupyter pyarrow
```

### 3. Adicione os arquivos de dados
Coloque os CSVs na pasta `data/raw/`:
```
data/raw/videos-stats.csv
data/raw/comments.csv
data/raw/USvideos.csv
```

### 4. Execute os notebooks em ordem
```bash
jupyter notebook
```
Abra e execute na sequência: `01` → `02` → `03` → `04` → `05`.

> Cada notebook salva seus resultados como parquet em `data/processed/`, servindo de entrada para o notebook seguinte.

---

## 🔄 Fluxo do Pipeline

```
videos-stats.csv ──┐
comments.csv ──────┤──► 01-leitura-escrita ──► videos-parquet/
USvideos.csv ──────┘
                              │
                              ▼
                    02-tratamento ──► videos-tratados-parquet/
                                      videos-comments-tratados-parquet/
                              │
                              ▼
                    03-preparacao ──► videos-preparados-parquet/
                              │
                              ▼
                    04-agregacao  (análises / sem output em disco)
                              │
                              ▼
                    05-otimizacao ──► join-videos-comments-otimizado/
```

---

## 📈 Principais Resultados

- **41 keywords** categorizadas e indexadas numericamente
- **15 títulos duplicados** identificados no dataset
- **90,1% da variância** capturada pelo primeiro componente PCA
- Join otimizado com **Broadcast Hash Join** elimina shuffle no maior DataFrame
- Dataset final integrado: **18.409 registros** com metadados de vídeo + comentários

---

## 📄 Licença

Os dados utilizados estão sob licença **CC0 (domínio público)**. O código deste repositório está sob licença **MIT**.
