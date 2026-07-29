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

| Área                             | Situação                                                        |
| -------------------------------- | --------------------------------------------------------------- |
| Emendas                          | ✅ Pipeline completo                                             |
| Favorecidos                      | ✅ Pipeline completo                                             |
| Convênios                        | ✅ Pipeline completo                                             |
| Contratos                        | ✅ Pipeline completo                                             |
| Licitações                       | ✅ Pipeline completo                                             |
| Gastos dos deputados             | ✅ Pipeline completo                                             |
| Proposições e votações da Câmara | ✅ Pipeline completo e publicado para consumo no Power BI        |
| Atividade e gastos do Senado     | 🟡 Próxima etapa — mapeamento e Bronze                           |
| Camada de consumo Power BI       | 🟡 44 dimensões e 40 fatos, rankings e relacionamentos publicados |
| Projeto PBIP versionável         | ✅ Criado e versionado                                           |
| Modelo semântico                 | 🟡 Estrutura inicial                                             |
| Dashboards                       | ❌ Não iniciados                                                 |

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
