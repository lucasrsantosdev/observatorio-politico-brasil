# Observatório Político Brasil

Projeto de engenharia de dados para extração, armazenamento, processamento e análise de dados públicos do Governo Federal.

## Configuração inicial

Antes de executar o projeto, cada colaborador deve configurar suas próprias variáveis de ambiente e gerar sua própria chave de acesso à API do Portal da Transparência.

> **Importante:** nunca publique chaves, senhas, tokens ou arquivos `.env` no GitHub.

### 1. Criar o arquivo de variáveis de ambiente

Na raiz do projeto, copie o arquivo `exemple_env.txt` e renomeie a cópia para `.env`.

#### PowerShell — Windows

```powershell
Copy-Item .\exemple_env.txt .\.env
```

#### Linux ou macOS

```bash
cp exemple_env.txt .env
```

O arquivo `.env` deve permanecer somente na máquina do colaborador.

Confirme que o `.gitignore` contém:

```gitignore
.env
.env.*
!.env.example
```

### 2. Gerar a chave da API do Portal da Transparência

A chave deve ser gerada individualmente por cada colaborador no endereço oficial:

[Gerar chave de acesso à API do Portal da Transparência](https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email)

Para gerar a chave:

1. Acesse o endereço acima.
2. Clique na opção de autenticação pelo Gov.br.
3. Entre utilizando sua conta Gov.br.
4. Conclua a autenticação solicitada.
5. A chave será enviada para o e-mail cadastrado na conta Gov.br.
6. Copie a chave recebida e adicione-a ao arquivo `.env`.

O Portal da Transparência exige uma conta Gov.br de nível **Prata ou Ouro** ou, para autenticação por CPF e senha, que a **verificação em duas etapas esteja habilitada**. A chave é enviada para o e-mail vinculado à conta Gov.br.

Exemplo de configuração:

```env
PORTAL_TRANSPARENCIA_API_KEY=sua_chave_aqui
```

Nunca coloque a chave diretamente no código:

```python
# Não faça isso
API_KEY = "minha-chave-real"
```

Utilize sempre a variável de ambiente:

```python
import os

api_key = os.getenv("PORTAL_TRANSPARENCIA_API_KEY")

if not api_key:
    raise RuntimeError("A variável PORTAL_TRANSPARENCIA_API_KEY não foi configurada.")
```

### 3. Documentação da API

A documentação oficial dos endpoints está disponível em:

[Documentação Swagger da API do Portal da Transparência](https://api.portaldatransparencia.gov.br/)

Nessa documentação é possível consultar os endpoints, parâmetros obrigatórios, formatos de resposta e exemplos de requisição.

Informações gerais:

[API de Dados do Portal da Transparência](https://portaldatransparencia.gov.br/api-de-dados)

A API permite que aplicações consultem programaticamente os dados disponibilizados pelo Portal da Transparência.

## Objetivo

Construir uma plataforma auditável para coleta, tratamento e análise de dados públicos provenientes de:

* Portal da Transparência;
* Câmara dos Deputados;
* Senado Federal;
* outras fontes oficiais de dados públicos.

## Tecnologias

* Python;
* PostgreSQL;
* Power BI;
* APIs REST;
* arquitetura medalhão;
* processamento e validação de dados;
* rankings e indicadores políticos auditáveis.

## Escopo de análises

| Ranking ou análise                              | Fonte principal         |
| ----------------------------------------------- | ----------------------- |
| Emendas, valores empenhados, liquidados e pagos | Portal da Transparência |
| Favorecidos e empresas recebedoras              | Portal da Transparência |
| Contratos e licitações federais                 | Portal da Transparência |
| Gastos, proposições e votações dos deputados    | Câmara dos Deputados    |
| Atividade legislativa e gastos dos senadores    | Senado Federal          |

## Status do projeto

| Área                             | Situação                                                 |
| -------------------------------- | -------------------------------------------------------- |
| Emendas                          | ✅ Pipeline completo                                      |
| Favorecidos                      | ✅ Pipeline completo                                      |
| Convênios                        | ✅ Pipeline completo                                      |
| Contratos                        | ✅ Pipeline completo                                      |
| Licitações                       | ✅ Pipeline completo                                      |
| Gastos dos deputados             | ✅ Pipeline completo                                      |
| Proposições e votações da Câmara | ✅ Pipeline completo e publicado para consumo no Power BI |
| Atividade e gastos do Senado     | ✅ Pipeline completo e publicado para consumo no Power BI |
| Projeto PBIP versionável         | ✅ Criado e versionado                                    |
| Modelo semântico                 | 🟡 Estrutura inicial                                      |
| Dashboards                       | ❌ Não iniciados                                          |

## Solicitação de documentos ausentes

Quando um documento ou conjunto de dados públicos necessário para o projeto não estiver disponível nos portais oficiais, deverá ser registrada uma solicitação pelo **Fala.BR**:

[Acessar o Fala.BR](https://falabr.cgu.gov.br/web/home)

O Fala.BR é a plataforma oficial do Poder Executivo Federal para pedidos de acesso à informação e manifestações de ouvidoria.

### Procedimento recomendado

1. Identifique o órgão responsável pelos documentos.
2. Registre um **Pedido de Acesso à Informação** solicitando os arquivos, dados ou documentos ausentes.
3. Descreva claramente:

   * quais documentos estão sendo solicitados;
   * qual período deve ser contemplado;
   * qual órgão ou unidade produziu os dados;
   * em qual formato os dados devem ser fornecidos, preferencialmente CSV, JSON, XLSX ou outro formato aberto;
   * onde os dados deveriam estar publicados;
   * a finalidade de pesquisa, fiscalização ou controle social.
4. Guarde o número de protocolo.
5. Registre o protocolo e seu andamento na documentação do projeto.
6. Quando aplicável, registre também uma manifestação de ouvidoria sobre a indisponibilidade dos dados.

### Pedido de informação ou denúncia?

Utilize:

* **Pedido de Acesso à Informação:** quando o objetivo for obter documentos ou dados que ainda não foram localizados.
* **Reclamação ou solicitação de providências:** quando os documentos deveriam estar publicados, mas estão indisponíveis, incompletos ou desatualizados.
* **Denúncia:** somente quando existirem indícios concretos de irregularidade, omissão intencional, descumprimento ou outra conduta que precise ser apurada.

O Fala.BR aceita pedidos de acesso à informação, denúncias, reclamações, sugestões e solicitações.

### Registro de protocolos no projeto

Os protocolos poderão ser documentados em:

```text
docs/
└── solicitacoes_falabr/
    ├── README.md
    ├── pedidos_acesso_informacao.csv
    └── evidencias/
```

Exemplo de controle:

| Protocolo   | Órgão       | Assunto             | Data da solicitação | Situação     | Resposta   |
| ----------- | ----------- | ------------------- | ------------------- | ------------ | ---------- |
| A preencher | A preencher | Documentos ausentes | A preencher         | Em andamento | Aguardando |

Não publique no repositório informações pessoais, CPF, endereço, e-mail particular ou outros dados sensíveis presentes nos protocolos.

## Segurança e colaboração

Cada colaborador deve:

* utilizar sua própria chave da API;
* manter o arquivo `.env` fora do controle de versão;
* não compartilhar tokens em commits, issues, pull requests ou mensagens públicas;
* revogar e substituir imediatamente qualquer chave exposta;
* documentar novas variáveis apenas no arquivo de exemplo, sem incluir valores reais;
* validar os dados utilizando fontes oficiais;
* registrar a origem, a data de extração e as regras de transformação;
* manter as análises reproduzíveis e auditáveis.

## Contribuição

Antes de iniciar uma alteração:

1. Faça um fork ou clone do repositório.
2. Configure seu arquivo `.env`.
3. Gere sua própria chave de acesso.
4. Crie uma branch específica para a alteração.
5. Execute os testes e validações locais.
6. Abra um pull request explicando:

   * o objetivo da alteração;
   * a fonte dos dados;
   * as regras implementadas;
   * as validações realizadas;
   * eventuais limitações encontradas.

Toda análise deve manter neutralidade política, rastreabilidade das fontes e possibilidade de reprodução dos resultados.

## Power BI

O projeto utiliza o formato **Power BI Project (`.pbip`)**, permitindo versionar no Git o relatório e o modelo semântico.

### Pré-requisitos

Para abrir o relatório, é necessário ter o **Power BI Desktop** instalado.

O projeto Power BI utiliza arquivos Parquet gerados localmente pelos pipelines. Por isso, antes de abrir ou atualizar o relatório, execute os pipelines e scripts de publicação responsáveis por gerar os arquivos dentro de:

```text
output/power_bi/

Os arquivos Parquet e CSV não são versionados no Git, pois podem ser reconstruídos a partir das fontes oficiais.

Estrutura do projeto Power BI
painel_portal_transparencia.pbip
painel_portal_transparencia.Report/
painel_portal_transparencia.SemanticModel/
painel_portal_transparencia.pbip: arquivo utilizado para abrir o projeto no Power BI Desktop;
painel_portal_transparencia.Report: páginas, visuais e configurações do relatório;
painel_portal_transparencia.SemanticModel: tabelas, medidas DAX e relacionamentos do modelo semântico.
Gerar os dados para o Power BI

Na raiz do projeto, execute os pipelines necessários e publique os arquivos de consumo.

Senado Federal
uv run python -m observatorio_politico.main senado-bronze
uv run python -m observatorio_politico.main senado-silver
uv run python -m observatorio_politico.main senado-gold
uv run python -m observatorio_politico.main senado-quality
uv run python -m observatorio_politico.main senado-dimensions

uv run python .\scripts\publicar_senado_power_bi.py
Gastos dos deputados

Execute o pipeline de gastos dos deputados e confirme que os arquivos foram publicados em:

output/power_bi/gastos_deputados/
Abrir o projeto

Com os arquivos de consumo gerados, abra o projeto pelo arquivo:

Start-Process .\painel_portal_transparencia.pbip

Também é possível abrir manualmente o arquivo:

painel_portal_transparencia.pbip

Ao abrir o Power BI Desktop:

Aguarde o carregamento do modelo semântico;
Caso apareça uma mensagem sobre alterações externas, aceite recarregar o projeto;
Clique em Atualizar agora;
Aguarde a leitura dos arquivos Parquet;
Salve o projeto após as alterações.
Observação sobre caminhos locais

O modelo semântico utiliza arquivos Parquet armazenados localmente no projeto.

Caso o projeto seja clonado em outro diretório, execute novamente os geradores do modelo semântico para atualizar os caminhos dos arquivos:

uv run python .\scripts\gerar_modelo_semantico_senado.py
uv run python .\scripts\gerar_modelo_semantico_gastos_deputados.py
uv run python .\scripts\gerar_relacionamentos_senado.py

Depois abra novamente:

Start-Process .\painel_portal_transparencia.pbip
Arquivos que não são versionados

Os seguintes arquivos são gerados localmente e não devem ser enviados ao Git:

output/power_bi/**/*.parquet
output/power_bi/**/*.csv
output/auditoria/
output/backup_modelo_semantico/
**/.pbi/
*.abf

O código dos pipelines, o modelo semântico, as medidas, os relacionamentos e as páginas do relatório permanecem versionados.