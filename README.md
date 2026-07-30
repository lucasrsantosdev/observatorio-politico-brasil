<div align="center">

🇧🇷 Observatório Político Brasil

Engenharia de dados aplicada à transparência pública

Plataforma open source para coleta, organização, validação e análise de dados públicos oficiais, com pipelines reproduzíveis, arquitetura medalhão e modelo analítico versionável no Power BI.



Portal da Transparência • Câmara dos Deputados • Senado Federal

</div>

Sumário

Visão geral

Princípios do projeto

Arquitetura

Fontes e domínios

Status

Tecnologias

Estrutura do repositório

Configuração do ambiente

Execução dos pipelines

Qualidade e rastreabilidade

Power BI

Segurança

Contribuição

Solicitação de dados ausentes

Licença

Visão geral

O Observatório Político Brasil é um projeto de engenharia de dados voltado à consolidação de informações públicas provenientes de fontes oficiais do Governo Federal e do Poder Legislativo.

A plataforma transforma arquivos e respostas de APIs em conjuntos de dados analíticos organizados, validados e preparados para exploração no Power BI, em consultas SQL ou em outras ferramentas de análise.

O projeto foi estruturado para permitir:

reprodução integral dos pipelines;

rastreabilidade entre origem, transformação e resultado;

auditoria das regras de negócio;

versionamento do modelo semântico e do relatório;

evolução colaborativa por meio de código aberto;

neutralidade política na apresentação dos dados.

[!IMPORTANT]O projeto organiza dados oficiais para fins de análise, pesquisa e controle social.Ele não representa, endossa ou classifica partidos, candidatos, parlamentares ou instituições com base em preferência política.

Princípios do projeto

Princípio

Aplicação

Fonte oficial

Dados obtidos diretamente de APIs, arquivos e portais institucionais

Reprodutibilidade

Pipelines executáveis localmente a partir do código versionado

Rastreabilidade

Manifestos de execução, partições por período e registros de reconciliação

Qualidade

Validações entre as camadas Bronze, Silver e Gold

Transparência

Regras de transformação documentadas e resultados auditáveis

Neutralidade

Indicadores construídos sem orientação político-partidária

Segurança

Segredos e credenciais mantidos fora do controle de versão

Arquitetura

flowchart LR
    A1[Portal da Transparência]
    A2[Câmara dos Deputados]
    A3[Senado Federal]

    A1 --> B[Bronze<br/>dados brutos]
    A2 --> B
    A3 --> B

    B --> C[Silver<br/>padronização e qualidade]
    C --> D[Gold<br/>modelo analítico]

    D --> E1[(PostgreSQL)]
    D --> E2[Parquet / CSV]
    E1 --> F[Power BI]
    E2 --> F

    C -. validações .-> Q[Quality]
    D -. reconciliações .-> R[Reconciliation]

Arquitetura medalhão

🥉 Bronze — ingestão e preservação

A camada Bronze mantém os dados com o maior nível possível de fidelidade à origem.

Características:

arquivos originais de APIs e downloads;

particionamento por ano, mês, dia ou execução;

manifestos com informações da carga;

preservação de arquivos compactados quando aplicável;

possibilidade de reprocessamento sem nova consulta à origem.

🥈 Silver — padronização e qualidade

A camada Silver concentra as transformações técnicas necessárias para tornar os dados consistentes.

Exemplos:

normalização de nomes e tipos;

conversão de datas e valores monetários;

tratamento de nulos e duplicidades;

padronização de identificadores;

consolidação de múltiplas partições;

geração de relatórios de qualidade.

🥇 Gold — modelo analítico

A camada Gold disponibiliza estruturas prontas para consumo analítico.

Exemplos:

tabelas fato e dimensão;

rankings descritivos;

resumos mensais e anuais;

tabelas de relacionamento;

reconciliações entre camadas;

arquivos Parquet e CSV para consumo externo.

Fontes e domínios

Fonte

Domínios atualmente contemplados

Portal da Transparência

Emendas, favorecidos, convênios, contratos, licitações e órgãos SIAFI

Câmara dos Deputados

Gastos parlamentares, proposições, autores, temas, votações, orientações e votos

Senado Federal

Senadores, CEAPS, matérias, votações, fornecedores e empresas contratadas

Principais análises

Domínio

Exemplos de produtos analíticos

Emendas parlamentares

Valores empenhados, liquidados, pagos, favorecidos e distribuição territorial

Convênios

Convenentes, funções, localidades e relacionamento com emendas

Contratos

Contratados, órgãos, itens, termos aditivos e variações contratuais

Licitações

Órgãos, fornecedores, modalidades, participantes e itens

Câmara dos Deputados

Gastos, fornecedores, partidos, tipos de despesa, proposições e votos

Senado Federal

Despesas CEAPS, fornecedores, matérias, votações e atividade parlamentar

Status

Área

Situação

Emendas

✅ Pipeline implementado

Favorecidos

✅ Pipeline implementado

Convênios

✅ Pipeline implementado

Contratos

✅ Pipeline implementado

Licitações

✅ Pipeline implementado

Gastos dos deputados

✅ Pipeline implementado

Proposições e votações da Câmara

✅ Publicado para consumo analítico

Atividade e gastos do Senado

✅ Publicado para consumo analítico

Projeto Power BI no formato PBIP

✅ Versionado

Modelo semântico

🟡 Em evolução

Páginas e dashboards finais

🟡 Em desenvolvimento

Tecnologias

Categoria

Tecnologias

Linguagem

Python

Gerenciamento do projeto

uv, pyproject.toml

Processamento

Polars e bibliotecas do ecossistema Python

Persistência analítica

PostgreSQL

Formatos

Parquet, CSV e JSON

Integração

APIs REST e arquivos públicos

Qualidade

Validações, manifestos e reconciliações

Visualização

Power BI Desktop

Versionamento BI

Power BI Project (.pbip) e TMDL

Controle de versão

Git e GitHub

Estrutura do repositório

A estrutura abaixo apresenta uma visão lógica e reduzida do projeto. Pastas particionadas por ano, mês e execução foram omitidas para manter a documentação legível.

observatorio-politico-brasil/
├── data/
│   ├── bronze/
│   │   ├── camara_deputados/
│   │   │   ├── gastos_deputados/
│   │   │   └── proposicoes_votacoes/
│   │   ├── portal_transparencia/
│   │   │   ├── contratos/
│   │   │   ├── emendas/
│   │   │   ├── emendas_historico/
│   │   │   ├── emendas_convenios_historico/
│   │   │   ├── emendas_favorecidos_historico/
│   │   │   ├── licitacoes/
│   │   │   └── orgaos_siafi/
│   │   └── senado_federal/
│   ├── silver/
│   │   ├── camara_deputados/
│   │   ├── portal_transparencia/
│   │   └── senado_federal/
│   ├── gold/
│   │   ├── camara_deputados/
│   │   ├── portal_transparencia/
│   │   └── senado_federal/
│   └── rejected/
├── docs/
├── logs/
├── output/
│   ├── auditoria/
│   ├── backup_modelo_semantico/
│   └── power_bi/
├── painel_portal_transparencia.Report/
├── painel_portal_transparencia.SemanticModel/
│   └── definition/
│       ├── cultures/
│       ├── tables/
│       ├── database.tmdl
│       ├── model.tmdl
│       └── relationships.tmdl
├── powerbi/
├── scripts/
├── sql/
├── src/
├── tests/
├── exemple_env.txt
├── painel_portal_transparencia.pbip
├── pyproject.toml
├── uv.lock
└── README.md

Organização dos dados

Os dados seguem convenções de particionamento que facilitam reprocessamento, auditoria e leitura seletiva.

data/bronze/<fonte>/<dominio>/ano=<AAAA>/mes=<MM>/execucao=<TIMESTAMP>/
data/silver/<fonte>/<dominio>/periodo=<INTERVALO>/
data/gold/<fonte>/<dominio>/periodo=<INTERVALO>/

As estruturas Gold incluem, conforme o domínio:

dimensions/
fato_*/
ranking_*/
relacionamento_*/
resumo_*/
quality/
reconciliation/

Configuração do ambiente

Pré-requisitos

Git;

Python compatível com o pyproject.toml;

uv;

Power BI Desktop, apenas para uso do relatório;

PostgreSQL, quando a persistência em banco estiver habilitada;

conta Gov.br apta a gerar a chave da API do Portal da Transparência.

1. Clonar o repositório

git clone <URL_DO_REPOSITORIO>
cd observatorio-politico-brasil

2. Instalar as dependências

uv sync

3. Criar o arquivo de ambiente

PowerShell — Windows

Copy-Item .\exemple_env.txt .\.env

Linux ou macOS

cp exemple_env.txt .env

[!WARNING]O arquivo .env contém configurações locais e possíveis credenciais.Ele nunca deve ser enviado ao GitHub.

Confirme que o .gitignore contém:

.env
.env.*
!.env.example

4. Gerar a chave do Portal da Transparência

Cada colaborador deve gerar sua própria chave:

Cadastro de acesso à API

Documentação Swagger

Informações gerais da API

Fluxo recomendado:

Acesse a página de cadastro.

Autentique-se com sua conta Gov.br.

Conclua os requisitos de segurança solicitados.

Aguarde o recebimento da chave no e-mail vinculado à conta.

Grave a chave exclusivamente no arquivo .env.

Exemplo:

PORTAL_TRANSPARENCIA_API_KEY=sua_chave_aqui

Nunca grave credenciais diretamente no código:

# Incorreto
API_KEY = "chave-real"

Utilize variáveis de ambiente:

import os

api_key = os.getenv("PORTAL_TRANSPARENCIA_API_KEY")

if not api_key:
    raise RuntimeError(
        "A variável PORTAL_TRANSPARENCIA_API_KEY não foi configurada."
    )

Execução dos pipelines

Consulte os comandos disponíveis na CLI antes de executar uma carga:

uv run python -m observatorio_politico.main --help

Exemplo: Senado Federal

uv run python -m observatorio_politico.main senado-bronze
uv run python -m observatorio_politico.main senado-silver
uv run python -m observatorio_politico.main senado-gold
uv run python -m observatorio_politico.main senado-quality
uv run python -m observatorio_politico.main senado-dimensions

Publicação para consumo no Power BI:

uv run python .\scripts\publicar_senado_power_bi.py

Exemplo: gastos dos deputados

Execute a sequência Bronze, Silver, Gold, qualidade e publicação definida para o domínio. Ao final, confirme a geração dos arquivos em:

output/power_bi/gastos_deputados/

Testes

uv run pytest

Validação estática

Quando configurado no projeto:

uv run ruff check .
uv run mypy src

[!TIP]Antes de abrir um pull request, execute os testes, valide os manifestos e confira as reconciliações do domínio alterado.

Qualidade e rastreabilidade

O projeto adota controles de qualidade ao longo de todo o fluxo.

Manifestos

As execuções podem gerar arquivos como:

bronze.manifest.json
silver.manifest.json
gold.manifest.json
quality.manifest.json
reconciliation.manifest.json
dimensions.manifest.json
execucao.manifest.json

Esses arquivos registram informações úteis para auditoria, como:

período processado;

origem dos dados;

data e identificador da execução;

quantidade de arquivos ou registros;

artefatos produzidos;

resultado das validações.

Reconciliação

As tabelas de reconciliação permitem comparar etapas do pipeline e identificar divergências entre entrada, transformação e publicação.

Exemplos:

reconciliacao_emendas
reconciliacao_convenios
reconciliacao_contratos
reconciliacao_licitacoes
reconciliacao_gastos_deputados

Dados rejeitados

Registros que não atendem às regras mínimas de qualidade podem ser direcionados para:

data/rejected/

A rejeição deve ser acompanhada do motivo, da origem e da execução responsável.

Power BI

O relatório utiliza o formato Power BI Project (.pbip), que permite versionar separadamente o relatório e o modelo semântico.

Componentes

painel_portal_transparencia.pbip
painel_portal_transparencia.Report/
painel_portal_transparencia.SemanticModel/

Componente

Responsabilidade

painel_portal_transparencia.pbip

Ponto de entrada do projeto no Power BI Desktop

painel_portal_transparencia.Report/

Páginas, visuais, temas e configurações do relatório

painel_portal_transparencia.SemanticModel/

Tabelas, medidas DAX, culturas e relacionamentos

definition/tables/

Definições TMDL das tabelas e medidas

relationships.tmdl

Relacionamentos do modelo

model.tmdl

Configurações gerais do modelo semântico

Pré-requisitos

Antes de abrir ou atualizar o relatório:

instale o Power BI Desktop;

execute os pipelines necessários;

publique os arquivos de consumo;

confirme a existência dos arquivos em output/power_bi/.

Os arquivos Parquet e CSV de consumo não são versionados, pois podem ser reconstruídos a partir das fontes oficiais e dos pipelines.

Abrir o projeto

Start-Process .\painel_portal_transparencia.pbip

Também é possível abrir manualmente:

painel_portal_transparencia.pbip

Ao abrir o projeto:

aguarde o carregamento do modelo semântico;

aceite o recarregamento caso o Power BI detecte alterações externas;

selecione Atualizar agora;

aguarde a leitura dos arquivos Parquet;

salve o projeto após as alterações.

Atualização de caminhos locais

Como o modelo consome arquivos locais, um clone realizado em outro diretório pode exigir a regeneração das definições de origem:

uv run python .\scripts\gerar_modelo_semantico_senado.py
uv run python .\scripts\gerar_modelo_semantico_gastos_deputados.py
uv run python .\scripts\gerar_relacionamentos_senado.py

Depois, abra novamente:

Start-Process .\painel_portal_transparencia.pbip

Artefatos não versionados

output/power_bi/**/*.parquet
output/power_bi/**/*.csv
output/auditoria/
output/backup_modelo_semantico/
**/.pbi/
*.abf

O código dos pipelines, as definições TMDL, as medidas DAX, os relacionamentos e as páginas do relatório permanecem versionados.

Segurança

Cada colaborador deve:

utilizar sua própria chave de API;

manter o .env fora do controle de versão;

não compartilhar credenciais em commits, issues ou pull requests;

revogar imediatamente qualquer chave exposta;

documentar novas variáveis somente no arquivo de exemplo;

evitar dados pessoais desnecessários em logs e artefatos;

revisar arquivos antes de publicá-los no repositório.

[!CAUTION]Nunca publique chaves, senhas, tokens, cookies de sessão, arquivos .env ou informações pessoais obtidas em protocolos administrativos.

Contribuição

Contribuições técnicas, correções e novas fontes oficiais são bem-vindas.

Fluxo recomendado

Faça um fork ou clone do projeto.

Sincronize as dependências com uv sync.

Configure o .env.

Crie uma branch objetiva:

git checkout -b feature/nome-da-alteracao

Implemente a alteração.

Execute os testes e validações.

Atualize a documentação quando necessário.

Abra um pull request.

O pull request deve informar

objetivo da alteração;

fonte oficial utilizada;

período contemplado;

regras de transformação;

estratégia de particionamento;

validações realizadas;

impacto nas camadas Bronze, Silver e Gold;

impacto no modelo semântico;

limitações conhecidas.

Padrões esperados

código legível e modular;

funções com responsabilidade clara;

tratamento explícito de erros;

logs úteis para diagnóstico;

idempotência sempre que aplicável;

novas cargas acompanhadas de manifestos;

regras de negócio documentadas;

neutralidade política;

possibilidade de reprodução do resultado.

Solicitação de dados ausentes

Quando um documento ou conjunto de dados necessário não estiver disponível nos portais oficiais, poderá ser registrada uma solicitação no Fala.BR:

Acessar o Fala.BR

Quando utilizar

Tipo

Situação

Pedido de Acesso à Informação

Para solicitar documentos ou bases ainda não localizados

Reclamação ou solicitação

Quando dados que deveriam estar publicados estão indisponíveis ou desatualizados

Denúncia

Apenas quando houver indícios concretos de irregularidade que exijam apuração

Registro dos protocolos

docs/
└── solicitacoes_falabr/
    ├── README.md
    ├── pedidos_acesso_informacao.csv
    └── evidencias/

Exemplo:

Protocolo

Órgão

Assunto

Data

Situação

Resposta

A preencher

A preencher

Dados ausentes

A preencher

Em andamento

Aguardando

Não publique CPF, endereço, e-mail particular ou outros dados pessoais presentes nos protocolos.

Roadmap

Estrutura inicial dos pipelines

Arquitetura Bronze, Silver e Gold

Manifestos de execução

Camada de qualidade e reconciliação

Integração com Câmara dos Deputados

Integração com Senado Federal

Integração com Portal da Transparência

Projeto Power BI no formato PBIP

Consolidação final do modelo semântico

Desenvolvimento das páginas executivas

Automação completa de atualização

Ampliação de testes de qualidade

Documentação detalhada por domínio

Publicação de catálogo de dados

Licença

Este projeto é distribuído sob os termos definidos no arquivo LICENSE.

<div align="center">

Desenvolvido para fortalecer a transparência, a rastreabilidade e o acesso estruturado a dados públicos brasileiros.

</div>