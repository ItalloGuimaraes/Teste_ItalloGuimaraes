# Teste 1: Transformação de Dados ANS (ETL)

Este projeto implementa um pipeline de **ETL (Extract, Transform, Load)** automatizado para coletar, processar e consolidar dados de Despesas das Operadoras de Planos de Saúde, conforme disponibilizado no portal de Dados Abertos da ANS.

## 📋 Funcionalidades

O script executa as seguintes tarefas de forma autônoma:
1.  **Scraping Dinâmico:** Baixa os arquivos de demonstrações contábeis dos últimos trimestres.
2.  **Crawler de Enriquecimento (CADOP):** Acessa o diretório FTP da ANS, identifica a versão mais recente do arquivo `Relatorio_Cadop` (independente de mudanças de nomeclatura) e o utiliza para enriquecer os dados.
3.  **Tratamento de Encoding:** Detecta e corrige automaticamente problemas de codificação (`UTF-8`, `Latin-1`, `CP1252`) para evitar caracteres corrompidos (*mojibake*).
4.  **Análise de Qualidade:** Gera um log detalhado de inconsistências (valores negativos, zerados ou datas inválidas).
5.  **Consolidação:** Gera um arquivo final `.zip` contendo o CSV padronizado.

---

## 🚀 Como Executar

### Pré-requisitos
* Python 3.8 ou superior.
* Conexão com a internet (para baixar os dados da ANS).

### Passo a Passo

1.  **Instale as dependências:**
    Navegue até a pasta do projeto e execute:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Execute o Pipeline:**
    Utilize o script principal que orquestra todo o processo:
    ```bash
    python main.py
    ```

3.  **Resultado:**
    Ao final da execução, dois arquivos serão gerados na raiz:
    * 📦 `consolidado_despesas.zip`: O arquivo CSV final com os dados processados.
    * 📄 `relatorio_inconsistencias.txt`: Log contendo os alertas sobre a qualidade dos dados.

---

## 🛠️ Decisões Técnicas e Arquitetura

### 1. Enriquecimento de Dados (Data Enrichment)
Os arquivos originais de despesas utilizam apenas o código `REG_ANS` para identificar as operadoras. Para atender ao requisito de exibir **CNPJ** e **Razão Social**:
* Implementei um módulo que baixa a tabela auxiliar **CADOP** (Cadastro de Operadoras).
* O script cruza os dados (`Join`) usando o `REG_ANS` como chave primária.
* **Resiliência:** Caso a chave não bata exatamente (ex: zeros à esquerda), o algoritmo tenta normalizar a chave (`zfill`) para garantir o *match*.

### 2. Crawler de Resiliência (CADOP)
A ANS altera frequentemente o nome do arquivo de cadastro (ex: de `Relatorio_Cadop.csv` para `Relatorio_Cadop_Ativas.csv` ou `.zip`).
* **Solução:** Em vez de *hardcodar* a URL, criei um *crawler* que varre o diretório FTP, identifica o arquivo válido mais recente e obtém o link dinamicamente. Isso evita que o pipeline quebre com atualizações simples do portal.

### 3. Tratamento de Caracteres (Encoding)
Arquivos governamentais frequentemente misturam encodings (`UTF-8` e `Latin-1`).
* **Solução:** Implementei uma leitura com tratamento de exceção em cascata. O sistema tenta ler em `UTF-8`; se falhar, tenta `Latin-1` e `CP1252`.
* **Saída:** O arquivo final é salvo forçando `utf-8-sig` (com BOM), garantindo que acentos abram corretamente no **Excel** e editores de texto.

### 4. Análise de Inconsistências
Conforme solicitado, o sistema audita os dados e loga os seguintes cenários no arquivo `relatorio_inconsistencias.txt`:
* **Valores Negativos:** Alerta contábil.
* **Valores Zerados:** Alerta de qualidade de dado.
* **Inconsistência de Datas:** Trimestres não identificados.
* **Duplicidade de Cadastro:** Verifica se um mesmo CNPJ aparece associado a Razões Sociais diferentes ao longo dos arquivos.

---

## 📂 Estrutura do Projeto

```text
1_Leitura_Transformacao_Dados/
│
├── main.py                  # Ponto de entrada (Orquestrador)
├── requirements.txt         # Lista de bibliotecas necessárias
├── README.md                # Documentação do projeto
├── consolidado_despesas.zip # (Gerado após execução) Arquivo final
├── relatorio_inconsistencias.txt # (Gerado após execução) Logs de qualidade
│
└── src/                     # Código Fonte
    ├── __init__.py
    ├── scraper.py           # Módulo de download (Web Scraping)
    └── processor.py         # Módulo de ETL e Regras de Negócio
---

👤 Autor
Ítallo de Santana Guimarães
