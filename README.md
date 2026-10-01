# Plataforma de SLA e Inteligência de Operações de Campo

Pipeline de dados que responde, em segundos, a pergunta que uma prestadora de serviços de telecom não consegue responder rápido: **onde estamos estourando SLA, por quê, e quanto isso custa em multa.**

Arquitetura medalhão sobre 0,5 milhão de registros sintéticos de operação FTTH ordens de serviço, alarmes de rede óptica e medições diárias de potência terminando num modelo estrela e num painel interativo.

**(https://sparkling-druid-97c8ff.netlify.app/)**


## O problema

Uma prestadora de campo instala e repara fibra para operadoras de banda larga. Ela é cobrada por SLA: cada tipo de serviço tem um prazo contratual e uma multa por hora de atraso, com teto. Os dados vivem espalhados entre o sistema de OS, os apontamentos dos técnicos e os alarmes da rede, em formatos que não conversam.

Resultado: a empresa descobre que estourou o SLA quando chega a fatura da multa.

Este projeto constrói o caminho do dado bruto até a resposta.

---

## O que torna este projeto diferente: ele se prova

A maioria dos projetos de portfólio em dados termina em "construí um pipeline e um dashboard" sem nenhuma forma de saber se o resultado está certo.

Aqui o gerador sintético **esconde padrões de propósito** e grava o gabarito num arquivo separado (`_metadados.json`). O pipeline foi construído sem consultar esse arquivo. Só no final as duas listas foram comparadas.

| O que o gerador escondeu | O que o pipeline encontrou |
|---|---|
| 91 CTOs com defeito recorrente | **90** (entre as 95 sinalizadas como críticas) |
| 5 técnicos com mais reincidência | **4 nas 4 primeiras posições**; o 5º em 7º de 41 |
| Crise de pessoal em SP Zona Leste a partir de ~25/01/2026 | queda de **79,5% para 24,1%** de conformidade entre jan e mar |
| Renegociação do contrato CLI01 em 01/03/2026 | SLA de reparo **20h → 12h** no mês exato, conformidade caindo de 51,9% para 21,1% |

E a sujeira injetada na origem, toda capturada e contabilizada:

| Problema | Injetado | Capturado |
|---|---|---|
| Linhas duplicadas (reprocessamento de extração) | 600 | 600 |
| Encerramento antes da chegada (campos trocados) | 133 | 133 |
| OS concluída sem técnico atribuído | 399 | 399 |
| Código de serviço em caixa baixa | 750 | 750 |
| Data de abertura em formato `dd/mm/aaaa` | 750 | 750 |
| CEP sem hífen | 2.590 | 2.590 |
| Bairro com caixa, acento ou espaço inconsistente | 1.500 | 1.219 realmente divergentes |

> O número de bairros é menor porque o gerador sorteia transformações que às vezes não alteram nada — aplicar `upper()` num bairro já em caixa alta, por exemplo. A silver absorveu 100% das divergências reais.


## Arquitetura

dados_telecom/   BRONZE   10 tabelas · CSV bruto, como sairia dos sistemas de origem
     ↓  PySpark 4.2 — limpeza, deduplicação, tipagem, cálculo de tempos
silver/          SILVER   10 tabelas · Parquet particionado por mês
     ↓  DuckDB — modelagem dimensional
gold/            GOLD     14 tabelas · modelo estrela, 0 chaves órfãs
     ↓
telecom.duckdb   SERVIÇO  banco único para o painel e ferramentas de BI


**Volume:** 50.000 ordens de serviço · 31.574 alarmes de rede · 438.000 medições diárias de potência óptica · 28.779 assinantes · 1.200 CTOs · 7 regiões · 4 operadoras contratantes · 12 meses.

### Modelo estrela

| Fatos | Grão |
|---|---|
| `fato_ordem_servico` | 1 ordem de serviço |
| `fato_alarme_rede` | 1 alarme de OLT/CTO/ONU |
| `fato_saude_cto` | CTO × dia |
| `fato_mapa_calor` | CTO × mês |
| `fato_reincidencia_cto` | OS de reparo |
| `fato_retrabalho_tecnico` | reparo concluído |

Dimensões: `dim_tempo`, `dim_cliente`, `dim_regiao`, `dim_cto`, `dim_cto_risco`, `dim_tecnico`, `dim_tipo_servico`, `dim_alarme_preditivo`.

---

## Indicadores

**73,3%** de conformidade de SLA no período · **R$ 377.490,81** de multa estimada · **42,8h** de tempo médio de atendimento · **95 CTOs** sinalizadas como críticas.

### Alarme que vira reparo

O cruzamento entre alarmes de rede e OS de reparo, medido contra a linha de base real — **18,1%** de qualquer dia-CTO já é seguido por um reparo em até 3 dias:

| Alarme | Severidade | Vira reparo em 3d | Lift |
|---|---|---|---|
| `PON_DOWN` | CRITICAL | 100,0% | **5,52×** |
| `LOS` | MAJOR | 77,1% | **4,26×** |
| `LOW_RX_POWER` | WARNING | 56,8% | **3,13×** |
| `DYING_GASP` | MINOR | 30,6% | 1,69× |
| `HIGH_BER` | WARNING | 25,9% | 1,43× |

`LOW_RX_POWER` e `HIGH_BER` carregam a **mesma severidade**, mas um prevê reparo duas vezes melhor que o outro. Para priorizar despacho de equipe, o tipo do alarme vale mais que a severidade que o equipamento reporta.

---

## Decisões e armadilhas

As partes que custaram tempo, e o que elas ensinaram.

**ANSI mode quebrou o parsing de datas.** O Spark 4 liga `spark.sql.ansi.enabled` por padrão: `to_timestamp` passa a lançar exceção em vez de devolver `null`. O idioma `coalesce(parse_iso, parse_br)`, que funciona no Spark 3 e no Databricks, derruba o job na primeira linha em formato brasileiro. Solução: `try_to_timestamp`, que declara explicitamente onde a falha é esperada e preserva o rigor do ANSI no resto.

**`least()` ignora nulos — e inventou R$ 228 mil em multas.** A multa é `min(horas_atraso × valor_hora, teto)`. Em OS canceladas o atraso é nulo, e `least(NULL, teto)` **não devolve nulo**: devolve o teto. Resultado: 5.626 ordens que nunca deveriam ser aferidas receberam o valor máximo de multa, silenciosamente. Só apareceu porque a validação cruzou a soma da coluna com a soma filtrada. A correção é um `when()` externo que decide antes quem entra no cálculo.

**O grão errado mede a coisa errada.** A primeira versão do indicador de técnico contava reparos anteriores na mesma CTO. Taxa base de 78% — a métrica saturou e apontou os técnicos errados, porque reincidência de CTO é propriedade do **ativo físico**, não de quem o atendeu. Trocando o grão para *mesmo assinante* em janela de 7 dias, a taxa base caiu para 10% e os cinco técnicos do gabarito apareceram no topo.

**Limiar chutado não dispara.** O detector de degradação óptica começou com −1,5 dB por ser um número plausível em FTTH. O mínimo da série inteira era −1,40: o alarme nunca poderia tocar. Calibrando contra o gabarito, o sinal que separa CTOs boas de ruins não é a potência média (11,8% de precisão) e sim o **percentual de ONUs online** (96,8%). Um score composto ponderando vários sinais ficou *pior* que o sinal simples — combinar variáveis correlacionadas adicionou ruído, não informação.

**`to_parquet` com partição acrescenta, não sobrescreve.** Rodar a célula de gravação duas vezes dobrou duas tabelas: 50.000 OS viraram 100.000, com cada chave aparecendo exatamente duas vezes. O `pandas.to_parquet(partition_cols=...)` escreve arquivos novos com nome UUID dentro das partições existentes. Toda função de escrita idempotente precisa limpar o destino antes.

**Por que a gold é DuckDB e não Spark.** No Windows, o Spark depende do `winutils.exe` para operações de arquivo do Hadoop — sem ele, não lê nem escreve Parquet. A alternativa de ponte via pandas corrompia nulos em `NaN`, contaminando toda agregação. O DuckDB lê a silver particionada em 0,2 segundo, preserva os nulos e entrega SQL que migra direto para dbt. Bronze e silver em PySpark, gold em SQL é um desenho comum em produção, não um remendo.

---

## Reproduzir

Requisitos: Python 3.12, Java 17+ (para o PySpark).

```bash
pip install pandas numpy faker pyarrow pyspark duckdb
```

**1. Gerar a camada bronze.** A semente é fixa — a saída é idêntica em qualquer máquina, em cerca de 15 segundos:

```bash
python gerador_dados_telecom.py
```

Opções úteis: `--ordens 200000 --meses 24` para mais volume, `--sujeira 0` para dados limpos, `--sem-medicoes` para pular a tabela maior, `--formato parquet`.

**2. Construir a silver** — execute `02_silver.ipynb` do início ao fim. O notebook cria a `SparkSession`, lê os CSVs, limpa, tipa e grava em `silver/`.

**3. Construir a gold** — execute `03_gold.ipynb`. Lê a silver com DuckDB, monta o modelo estrela em `gold/` e publica `telecom.duckdb`.

As camadas `dados_telecom/`, `silver/`, `gold/` e o arquivo `.duckdb` não são versionados: são saída determinística do código acima.

---

## Estrutura

```
gerador_dados_telecom.py   gerador sintético (bronze + gabarito)
02_silver.ipynb            limpeza, tipagem, cálculo de tempos e SLA
03_gold.ipynb              modelo estrela, reincidência, risco de CTO
powerbi/                   .pbids, consultas Power Query (M) e medidas DAX
painel.html                dashboard interativo (fonte do artifact)
```

---

## Stack

PySpark 4.2 · DuckDB 1.5 · Parquet · pandas · PyArrow · Faker · HTML/SVG sem dependências

---

## Observações

Todos os dados são **sintéticos**. Nomes, endereços e CNPJs são gerados por Faker; nenhuma informação real de cliente ou de operadora está envolvida.

O desenho se aplica sem mudanças a dados reais de um sistema de OS — a camada bronze é o único ponto que muda.
