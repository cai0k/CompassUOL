### 1. É um framework de processamento de dados em memória que oferece um ambiente de computação distribuída para análise e processamento de big data. 

### 2. Processamento em Memória:
### 3. O processamento em memória permite processar grandes volumes de dados de forma muito mais rápida do que sistemas tradicionais, como o Hadoop MapReduce.

### 4. Computação Distribuída
### 5. Oferece uma estrutura para o processamento distribuído, dividindo tarefas em pequenas unidades de trabalho que podem ser executadas em várias máquinas, resultando em paralelismo eficiente.

### 6. APIs Versáteis
### 7. Oferece APIs em várias linguagens, incluindo Scala, Java, Python (PySpark) e R, tornando-o acessível a um grande número de desenvolvedores.

### 8. Componentes:
### 9. O ecossistema do Spark inclui bibliotecas e ferramentas para processamento de streaming (Spark Streaming), machine learning (MLlib), análise de gráficos (GraphX) e processamento de SQL (Spark SQL).

### 10. Estrutura:
* Driver: SparkSession é iniciado, recursos computacionais do Cluster Manager são solicitados, operações são transformadas em DAGs distribuindo pelos executers
* Manager: Gerencia os recursos do cluster. Quatro possíveis: built-in standalone, YARN, Mesos e Kubernetes
* Executer: Executa tarefas em cada nó do cluster

### 11. Transformações e Ações:
* Todo dataframe é imutável e traz tolerância a falha
* A cada transformação gera um novo dataframe
* O processamento de transformação só ocorre quando há uma Ação

### 12. RDD–Resilient Distributed Datasets
* Dados imutáveis, distribuídos pelo cluster
* Tolerante a falhas
* Estrutura de baixo nível
* Pode persistir em disco
* É executado em memória
* Um novo RDD é criado através de operações de outro RDD

### 13. Dataframe
* Imutáveis
* Linhagem preservada
* Tabelas com linhas e colunas
* Colunas podem ter tipos diferentes
* Schema conhecido
* Análises comuns: Agrupar, ordenar, filtrar
* O spark otimiza estas analises através de planos de execução
