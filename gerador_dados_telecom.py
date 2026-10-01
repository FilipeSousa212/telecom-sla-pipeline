#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gerador de dados sintéticos: operações de campo de uma prestadora de serviços de telecom (FTTH)
==============================================================================================

Simula uma empresa que executa serviços de campo (instalação, reparo, retirada, manutenção de
rede) para operadoras de banda larga contratantes, sob contratos com SLA e multa por atraso.

Tabelas geradas (camada "bronze", como viriam dos sistemas de origem):
  dim_regiao            regiões de atuação
  clientes_operadoras   operadoras contratantes
  tipos_servico         catálogo de serviços
  contratos_sla         SLA, valor e multa por cliente x serviço (com versões e vigência)
  tecnicos              técnicos de campo (com admissões e desligamentos)
  ctos                  caixas de terminação óptica (rede FTTH)
  assinantes            clientes finais atendidos
  ordens_servico        OS com o ciclo completo: abertura, despacho, chegada, encerramento
  alarmes_rede          alarmes de OLT/CTO/ONU (LOS, dying gasp, baixa potência...)
  medicoes_potencia     leitura diária de potência óptica por CTO

Padrões escondidos nos dados (para descobrir na análise; o gabarito fica em _metadados.json):
  - sazonalidade: mais reparos e rompimentos no período de chuvas (dez-mar)
  - OS abertas na sexta à noite ou no fim de semana estouram mais o SLA (expediente dos técnicos)
  - uma região perde técnicos de uma vez ("crise de pessoal") e o tempo de atendimento piora
  - algumas CTOs são problemáticas: concentram reparos, reincidências e degradação de potência
  - alguns técnicos geram mais reincidência (retorno ao mesmo assinante em até 30 dias)
  - a operadora CLI01 renegociou o contrato no meio do período (SLA de reparo mais apertado),
    o que pede histórico de vigência (SCD tipo 2) no modelo
  - quedas de energia geram rajadas de DYING_GASP numa região inteira
  - a potência óptica das CTOs degrada até uma manutenção e volta ao normal depois

Sujeira proposital (--sujeira) para exercitar a camada silver:
  duplicatas, texto com caixa/acentos/espaços inconsistentes, datas em formato misto,
  técnico nulo em OS concluída, encerramento antes da chegada, códigos em minúsculo e
  CEP sem hífen. A contagem do que foi injetado fica em _metadados.json.

Uso:
  pip install pandas numpy faker pyarrow
  python gerador_dados_telecom.py                        # 12 meses, ~50 mil OS, CSV em ./dados_telecom
  python gerador_dados_telecom.py --ordens 200000 --meses 24 --formato parquet
  python gerador_dados_telecom.py --sujeira 0            # dados limpos
  python gerador_dados_telecom.py --sem-medicoes         # pula a tabela maior (medições diárias)
"""

from __future__ import annotations

import argparse
import heapq
import json
import math
import random
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------------------------
# Parâmetros de negócio
# ---------------------------------------------------------------------------------------------

REGIOES = [
    {"id_regiao": "RJ-CAP-N", "nome": "Rio de Janeiro - Zona Norte", "cidade": "Rio de Janeiro", "uf": "RJ",
     "lat": -22.870, "lon": -43.300, "peso": 0.18, "fator_desloc": 1.2,
     "bairros": ["Tijuca", "Méier", "Madureira", "Penha", "Vila Isabel", "Irajá"]},
    {"id_regiao": "RJ-CAP-S", "nome": "Rio de Janeiro - Zona Sul/Centro", "cidade": "Rio de Janeiro", "uf": "RJ",
     "lat": -22.950, "lon": -43.190, "peso": 0.14, "fator_desloc": 1.4,
     "bairros": ["Copacabana", "Botafogo", "Flamengo", "Centro", "Laranjeiras", "Ipanema"]},
    {"id_regiao": "RJ-CAP-O", "nome": "Rio de Janeiro - Zona Oeste", "cidade": "Rio de Janeiro", "uf": "RJ",
     "lat": -22.940, "lon": -43.480, "peso": 0.16, "fator_desloc": 1.5,
     "bairros": ["Barra da Tijuca", "Campo Grande", "Jacarepaguá", "Recreio", "Bangu", "Santa Cruz"]},
    {"id_regiao": "RJ-NIT", "nome": "Niterói", "cidade": "Niterói", "uf": "RJ",
     "lat": -22.900, "lon": -43.100, "peso": 0.09, "fator_desloc": 1.0,
     "bairros": ["Icaraí", "Centro", "Fonseca", "Santa Rosa", "Pendotiba"]},
    {"id_regiao": "SP-CAP-L", "nome": "São Paulo - Zona Leste", "cidade": "São Paulo", "uf": "SP",
     "lat": -23.550, "lon": -46.500, "peso": 0.17, "fator_desloc": 1.3,
     "bairros": ["Tatuapé", "Mooca", "Itaquera", "Penha", "Vila Formosa", "São Mateus"]},
    {"id_regiao": "SP-CAMP", "nome": "Campinas", "cidade": "Campinas", "uf": "SP",
     "lat": -22.900, "lon": -47.060, "peso": 0.11, "fator_desloc": 1.0,
     "bairros": ["Cambuí", "Taquaral", "Barão Geraldo", "Castelo", "Centro"]},
    {"id_regiao": "MG-BH", "nome": "Belo Horizonte", "cidade": "Belo Horizonte", "uf": "MG",
     "lat": -19.920, "lon": -43.940, "peso": 0.15, "fator_desloc": 1.1,
     "bairros": ["Savassi", "Pampulha", "Buritis", "Centro", "Barreiro", "Venda Nova"]},
]
REGIAO_CRISE = "SP-CAP-L"

# Operadoras contratantes (fictícias). fator_sla < 1 = contrato mais exigente.
OPERADORAS = [
    {"id_cliente": "CLI01", "razao_social": "Alfa Fibra Telecomunicações S.A.", "nome_fantasia": "Alfa Fibra",
     "peso": 0.40, "fator_sla": 0.85},
    {"id_cliente": "CLI02", "razao_social": "Conecta Mais Internet Ltda.", "nome_fantasia": "Conecta Mais",
     "peso": 0.27, "fator_sla": 1.00},
    {"id_cliente": "CLI03", "razao_social": "NovaLink Telecom Ltda.", "nome_fantasia": "NovaLink",
     "peso": 0.20, "fator_sla": 1.00},
    {"id_cliente": "CLI04", "razao_social": "Rede Horizonte Banda Larga Ltda.", "nome_fantasia": "Rede Horizonte",
     "peso": 0.13, "fator_sla": 1.20},
]

# espera_med_h = mediana (horas) entre abertura e chegada do técnico em condição normal
TIPOS_SERVICO = {
    "INST_FTTH": dict(descricao="Instalação FTTH", grupo="ATIVACAO", peso=0.34, sla_h=72, dur_min=120,
                      espera_med_h=30, valor=180.0, p_cancel=0.07, p_improd=0.08),
    "REP_FTTH": dict(descricao="Reparo FTTH - sem conexão", grupo="REPARO", peso=0.30, sla_h=24, dur_min=75,
                     espera_med_h=7, valor=95.0, p_cancel=0.03, p_improd=0.05),
    "REP_LENT": dict(descricao="Reparo FTTH - lentidão/sinal degradado", grupo="REPARO", peso=0.12, sla_h=48,
                     dur_min=60, espera_med_h=14, valor=85.0, p_cancel=0.04, p_improd=0.05),
    "MUD_END": dict(descricao="Mudança de endereço", grupo="ATIVACAO", peso=0.08, sla_h=96, dur_min=110,
                    espera_med_h=36, valor=150.0, p_cancel=0.05, p_improd=0.06),
    "RET_EQP": dict(descricao="Retirada de equipamento", grupo="RETIRADA", peso=0.08, sla_h=120, dur_min=30,
                    espera_med_h=50, valor=45.0, p_cancel=0.08, p_improd=0.12),
    "MAN_PREV": dict(descricao="Manutenção preventiva de CTO", grupo="REDE", peso=0.05, sla_h=168, dur_min=90,
                     espera_med_h=70, valor=130.0, p_cancel=0.02, p_improd=0.0),
    "REP_ROMP": dict(descricao="Reparo de rompimento de cabo", grupo="REDE", peso=0.03, sla_h=8, dur_min=240,
                     espera_med_h=1.2, valor=650.0, p_cancel=0.0, p_improd=0.0),
}

MESES_CHUVA = {12, 1, 2, 3}
PESO_DIA_SEMANA = [1.20, 1.08, 1.05, 1.03, 1.00, 0.62, 0.30]  # seg..dom
PESO_HORA = [0.2, 0.1, 0.1, 0.1, 0.1, 0.3, 0.8, 2.0, 4.0, 5.0, 5.0, 4.5,
             3.5, 4.0, 4.5, 4.5, 4.0, 3.5, 3.0, 2.5, 2.0, 1.5, 1.0, 0.5]
NIVEL_FATOR = {"JUNIOR": 1.25, "PLENO": 1.00, "SENIOR": 0.85}  # multiplica a duração do serviço

CANAIS = ["URA", "APP", "CALL_CENTER", "PORTAL_OPERADORA"]
PESO_CANAIS = [0.30, 0.25, 0.25, 0.20]

CODIGOS_CANCELAMENTO = ["CANCELADO_PELO_ASSINANTE", "CANCELADO_PELA_OPERADORA", "DUPLICIDADE",
                        "REAGENDADO_FORA_DO_PRAZO"]
CAUSAS_REP_FTTH = {"ONU_DEFEITO": 0.18, "CONECTOR_SUJO": 0.15, "DROP_ROMPIDO": 0.20, "CTO_DEFEITO": 0.12,
                   "FONTE_ONU_QUEIMADA": 0.10, "CONFIG_LOGICA": 0.10, "SEM_DEFEITO_CONSTATADO": 0.15}
CAUSAS_REP_LENT = {"POTENCIA_BAIXA": 0.30, "CONECTOR_SUJO": 0.20, "WIFI_INTERFERENCIA": 0.20,
                   "EQUIPAMENTO_CLIENTE": 0.15, "CONFIG_LOGICA": 0.15}
CAUSAS_ROMP = {"ROMPIMENTO_OBRA": 0.30, "ROMPIMENTO_VEICULO": 0.20, "VANDALISMO": 0.15, "ROEDOR": 0.15,
               "INTEMPERIE": 0.20}
CAUSAS_IMPROD = {"CLIENTE_AUSENTE": 0.60, "ENDERECO_NAO_LOCALIZADO": 0.15, "ACESSO_NEGADO": 0.15,
                 "SEM_VIABILIDADE_TECNICA": 0.10}

CAUSA_ALARME = {"DROP_ROMPIDO": "LOS", "CTO_DEFEITO": "LOS", "ONU_DEFEITO": "LOS",
                "FONTE_ONU_QUEIMADA": "DYING_GASP", "CONECTOR_SUJO": "LOW_RX_POWER",
                "POTENCIA_BAIXA": "LOW_RX_POWER"}
SEVERIDADE = {"PON_DOWN": "CRITICAL", "LOS": "MAJOR", "DYING_GASP": "MINOR",
              "LOW_RX_POWER": "WARNING", "HIGH_BER": "WARNING"}


def _sem_acento(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c))


def _fmt_datas(df: pd.DataFrame, colunas: list[str], fmt: str = "%Y-%m-%d %H:%M:%S") -> None:
    """Converte colunas de data para texto (como numa extração bruta), mantendo nulos."""
    for c in colunas:
        s = pd.to_datetime(df[c])
        df[c] = s.dt.strftime(fmt).astype(object).where(s.notna(), None)


# ---------------------------------------------------------------------------------------------
# Gerador
# ---------------------------------------------------------------------------------------------

class GeradorTelecom:
    def __init__(self, args: argparse.Namespace):
        self.a = args
        self.rng = np.random.default_rng(args.seed)
        random.seed(args.seed)
        self.fake = Faker("pt_BR")
        Faker.seed(args.seed)

        self.inicio = datetime.fromisoformat(args.inicio)
        self.fim = (pd.Timestamp(self.inicio) + pd.DateOffset(months=args.meses)).to_pydatetime()
        self.dias = pd.date_range(self.inicio, self.fim - timedelta(days=1), freq="D")

        self.regiao_por_id = {r["id_regiao"]: r for r in REGIOES}
        self.peso_regiao = [r["peso"] for r in REGIOES]
        self.peso_cli = [c["peso"] for c in OPERADORAS]
        self.meta = {"parametros": vars(args), "sujeira_injetada": {}, "gabarito": {}}

    # ---- utilidades -------------------------------------------------------------------------
    def _u(self, a: float, b: float) -> float:
        return float(self.rng.uniform(a, b))

    def _sortear_regiao(self) -> dict:
        return random.choices(REGIOES, weights=self.peso_regiao)[0]

    def _sortear_cliente(self) -> str:
        return random.choices(OPERADORAS, weights=self.peso_cli)[0]["id_cliente"]

    @staticmethod
    def _ativo(tec: dict, quando: datetime) -> bool:
        return tec["data_admissao"] <= quando and (tec["data_desligamento"] is None
                                                  or tec["data_desligamento"] > quando)

    def _ass_ativo(self, idx: int, quando: datetime) -> bool:
        a = self.assinantes[idx]
        return (a["data_ativacao"] is not None and a["data_ativacao"] < quando
                and (a["data_cancelamento"] is None or a["data_cancelamento"] > quando))

    # ---- dimensões --------------------------------------------------------------------------
    def gerar_regioes(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "id_regiao": r["id_regiao"], "nome": r["nome"], "cidade": r["cidade"], "uf": r["uf"],
            "latitude_centro": r["lat"], "longitude_centro": r["lon"],
        } for r in REGIOES])

    def gerar_clientes(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "id_cliente": c["id_cliente"], "razao_social": c["razao_social"],
            "nome_fantasia": c["nome_fantasia"], "cnpj": self.fake.cnpj(),
            "data_inicio_relacionamento": (self.inicio - timedelta(days=int(self.rng.integers(400, 2500)))).date(),
        } for c in OPERADORAS])

    def gerar_tipos(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "codigo_tipo_servico": cod, "descricao": t["descricao"], "grupo": t["grupo"],
            "duracao_padrao_min": t["dur_min"],
        } for cod, t in TIPOS_SERVICO.items()])

    def gerar_contratos(self) -> pd.DataFrame:
        meio = self.inicio + (self.fim - self.inicio) / 2
        meio = datetime(meio.year, meio.month, 1)
        self.meta["gabarito"]["renegociacao_CLI01"] = {
            "data": meio.date(), "o_que_mudou": "SLA de reparo 40% menor e multa 50% maior"}
        linhas = []
        for c in OPERADORAS:
            for cod, t in TIPOS_SERVICO.items():
                sla = max(4, round(t["sla_h"] * c["fator_sla"]))
                valor = round(t["valor"] * self._u(0.90, 1.12), 2)
                multa = round(valor * self._u(0.02, 0.05), 2)
                base = dict(id_contrato=f"CT-{c['id_cliente']}-{cod}", versao=1, id_cliente=c["id_cliente"],
                            codigo_tipo_servico=cod, sla_horas=sla, valor_os=valor,
                            multa_por_hora_atraso=multa, teto_multa_pct=0.30,
                            vigencia_inicio=(self.inicio - timedelta(days=365)).date(), vigencia_fim=None)
                if c["id_cliente"] == "CLI01" and t["grupo"] == "REPARO":
                    base["vigencia_fim"] = (meio - timedelta(days=1)).date()
                    nova = dict(base, versao=2, sla_horas=max(4, round(sla * 0.6)),
                                multa_por_hora_atraso=round(multa * 1.5, 2),
                                vigencia_inicio=meio.date(), vigencia_fim=None)
                    linhas += [base, nova]
                else:
                    linhas.append(base)
        return pd.DataFrame(linhas)

    def _novo_tecnico(self, regiao: dict, admissao: datetime, nivel: str | None = None) -> dict:
        n = len(self.tecnicos) + 1
        tec = {
            "id_tecnico": f"TEC{n:04d}", "matricula": f"MAT{100000 + n}", "nome": self.fake.name(),
            "nivel": nivel or random.choices(list(NIVEL_FATOR), weights=[0.35, 0.45, 0.20])[0],
            "equipe": f"EQ-{regiao['id_regiao']}-{random.randint(1, 2)}",
            "id_regiao_base": regiao["id_regiao"], "data_admissao": admissao, "data_desligamento": None,
            "_qualidade_ruim": self.rng.random() < 0.10,
        }
        self.tecnicos.append(tec)
        self.tec_por_regiao[regiao["id_regiao"]].append(tec)
        return tec

    def gerar_tecnicos(self) -> pd.DataFrame:
        self.tecnicos, self.tec_por_regiao = [], defaultdict(list)
        n_dias = len(self.dias)
        n_total = max(2 * len(REGIOES), math.ceil(self.a.ordens / n_dias / 4.3))
        pesos = np.array(self.peso_regiao) / sum(self.peso_regiao)
        for r, qtd in zip(REGIOES, np.maximum(2, np.round(pesos * n_total).astype(int))):
            for _ in range(qtd):
                self._novo_tecnico(r, self.inicio - timedelta(days=int(self.rng.integers(30, 2200))))

        # crise de pessoal numa região: saída concentrada, reposição lenta
        data_crise = self.inicio + timedelta(days=int(n_dias * 0.4))
        for tec in list(self.tec_por_regiao[REGIAO_CRISE]):
            if self.rng.random() < 0.45:
                tec["data_desligamento"] = data_crise + timedelta(days=int(self.rng.integers(0, 21)))
                repos = tec["data_desligamento"] + timedelta(days=int(self.rng.integers(45, 90)))
                if repos < self.fim:
                    self._novo_tecnico(self.regiao_por_id[REGIAO_CRISE], repos, nivel="JUNIOR")
        self.meta["gabarito"]["crise_de_pessoal"] = {"regiao": REGIAO_CRISE, "inicio_aprox": data_crise.date()}

        # rotatividade normal
        for tec in list(self.tecnicos):
            if tec["data_desligamento"] is None and tec["data_admissao"] < self.inicio \
                    and self.rng.random() < self.a.turnover:
                saida = self.inicio + timedelta(days=int(self.rng.integers(15, max(16, n_dias - 15))))
                tec["data_desligamento"] = saida
                repos = saida + timedelta(days=int(self.rng.integers(20, 75)))
                if repos < self.fim:
                    self._novo_tecnico(self.regiao_por_id[tec["id_regiao_base"]], repos, nivel="JUNIOR")

        self.meta["gabarito"]["tecnicos_com_mais_reincidencia"] = sorted(
            t["id_tecnico"] for t in self.tecnicos if t["_qualidade_ruim"])
        self._cache_ativos: dict = {}
        return pd.DataFrame([{
            "id_tecnico": t["id_tecnico"], "matricula": t["matricula"], "nome": t["nome"], "nivel": t["nivel"],
            "equipe": t["equipe"], "id_regiao_base": t["id_regiao_base"],
            "data_admissao": t["data_admissao"].date(),
            "data_desligamento": t["data_desligamento"].date() if t["data_desligamento"] else None,
        } for t in self.tecnicos])

    def gerar_ctos(self) -> pd.DataFrame:
        self.ctos, self.ctos_por_regiao, self.peso_frag = [], defaultdict(list), {}
        pesos = np.array(self.peso_regiao) / sum(self.peso_regiao)
        for r, qtd in zip(REGIOES, np.maximum(10, np.round(pesos * self.a.ctos).astype(int))):
            n_olt = max(2, qtd // 150)
            for k in range(qtd):
                frag = self.rng.beta(5, 3) if self.rng.random() < 0.07 else self.rng.beta(1.2, 8)
                cto = {
                    "id_cto": f"CTO-{r['id_regiao']}-{k + 1:04d}",
                    "id_olt": f"OLT-{r['id_regiao']}-{chr(65 + k % n_olt)}",
                    "porta_pon": f"0/{self.rng.integers(1, 9)}/{self.rng.integers(1, 17)}",
                    "id_regiao": r["id_regiao"], "bairro": random.choice(r["bairros"]),
                    "cidade": r["cidade"], "uf": r["uf"],
                    "latitude": round(r["lat"] + self.rng.normal(0, 0.025), 6),
                    "longitude": round(r["lon"] + self.rng.normal(0, 0.025), 6),
                    "capacidade_portas": int(self.rng.choice([16, 32], p=[0.3, 0.7])),
                    "data_ativacao": (self.inicio - timedelta(days=int(self.rng.integers(60, 2500)))).date(),
                    "_frag": float(frag),
                }
                self.ctos.append(cto)
                self.ctos_por_regiao[r["id_regiao"]].append(cto)
        for rid, lista in self.ctos_por_regiao.items():
            self.peso_frag[rid] = [0.2 + 3 * c["_frag"] for c in lista]
        self.meta["gabarito"]["ctos_problematicas"] = sorted(
            c["id_cto"] for c in self.ctos if c["_frag"] > 0.45)
        return pd.DataFrame([{k: v for k, v in c.items() if not k.startswith("_")} for c in self.ctos])

    def _novo_assinante(self, cto: dict, id_cliente: str, data_ativacao: datetime | None) -> int:
        idx = len(self.assinantes)
        self.assinantes.append({
            "id_assinante": f"ASS{idx + 1:07d}", "id_cliente": id_cliente, "id_cto": cto["id_cto"],
            "nome": self.fake.name(), "logradouro": self.fake.street_name(),
            "numero": str(int(self.rng.integers(1, 3000))), "bairro": cto["bairro"],
            "cidade": cto["cidade"], "uf": cto["uf"],
            "cep": f"{int(self.rng.integers(20000, 99999))}-{int(self.rng.integers(0, 1000)):03d}",
            "plano_mbps": random.choices([300, 500, 600, 1000], weights=[0.25, 0.35, 0.25, 0.15])[0],
            "data_ativacao": data_ativacao, "data_cancelamento": None, "_cto": cto,
        })
        self.assin_por_cto[cto["id_cto"]].append(idx)
        return idx

    def gerar_base_assinantes(self) -> None:
        self.assinantes, self.assin_por_cto = [], defaultdict(list)
        for _ in range(self.a.assinantes):
            reg = self._sortear_regiao()
            cto = random.choice(self.ctos_por_regiao[reg["id_regiao"]])
            ativ = self.inicio - timedelta(days=int(self.rng.integers(30, 1500)),
                                           seconds=int(self.rng.integers(0, 86400)))
            self._novo_assinante(cto, self._sortear_cliente(), ativ)

    def tabela_assinantes(self) -> pd.DataFrame:
        linhas = []
        for a in self.assinantes:
            status = ("NAO_ATIVADO" if a["data_ativacao"] is None
                      else "CANCELADO" if a["data_cancelamento"] is not None else "ATIVO")
            linhas.append({**{k: v for k, v in a.items() if not k.startswith("_")}, "status": status})
        df = pd.DataFrame(linhas)
        _fmt_datas(df, ["data_ativacao", "data_cancelamento"], "%Y-%m-%d")
        return df

    # ---- ordens de serviço ------------------------------------------------------------------
    def _aberturas(self) -> list[datetime]:
        pesos = np.array([PESO_DIA_SEMANA[d.weekday()] * (1 + 0.012 * i / 30)
                          * (1.12 if d.month in MESES_CHUVA else 1.0) for i, d in enumerate(self.dias)])
        contagem = self.rng.multinomial(self.a.ordens, pesos / pesos.sum())
        ph = np.array(PESO_HORA) / sum(PESO_HORA)
        saida = []
        for d, c in zip(self.dias, contagem):
            if not c:
                continue
            horas = self.rng.choice(24, size=c, p=ph)
            segs = self.rng.integers(0, 3600, size=c)
            base = d.to_pydatetime()
            saida.extend(base + timedelta(seconds=int(h * 3600 + s)) for h, s in zip(horas, segs))
        saida.sort()
        return saida

    @staticmethod
    def _pesos_tipo(mes: int) -> list[float]:
        pesos = {c: t["peso"] for c, t in TIPOS_SERVICO.items()}
        if mes in MESES_CHUVA:
            pesos["REP_FTTH"] *= 1.35
            pesos["REP_LENT"] *= 1.20
            pesos["REP_ROMP"] *= 1.80
        if mes in (11, 12, 1):
            pesos["INST_FTTH"] *= 1.15
        return list(pesos.values())

    def _n_ativos(self, id_regiao: str, dia) -> int:
        chave = (id_regiao, dia)
        if chave not in self._cache_ativos:
            meio_dia = datetime(dia.year, dia.month, dia.day, 12)
            self._cache_ativos[chave] = sum(1 for t in self.tec_por_regiao[id_regiao]
                                            if self._ativo(t, meio_dia))
        return self._cache_ativos[chave]

    def _fator_carga(self, id_regiao: str, dia) -> float:
        backlog = sum(self.os_dia_regiao[(id_regiao, dia - timedelta(days=k))] for k in (1, 2, 3)) / 3
        if backlog == 0:
            return 1.0
        capacidade = max(1, self._n_ativos(id_regiao, dia)) * 4.3
        return float(np.clip((backlog / capacidade) ** 1.4, 0.75, 3.5))

    def _expediente(self, dt: datetime) -> datetime:
        """Empurra a chegada para o horário de trabalho (seg-sex 8h-17h, sáb 8h-12h30)."""
        for _ in range(10):
            wd, h = dt.weekday(), dt.hour + dt.minute / 60
            proximo_dia_8h = (dt + timedelta(days=1)).replace(hour=8, minute=0, second=0)
            atraso = timedelta(minutes=int(self.rng.integers(0, 90)))
            if wd == 6:
                dt = proximo_dia_8h + atraso
            elif h < 8:
                dt = dt.replace(hour=8, minute=0, second=0) + atraso
            elif h >= (12.5 if wd == 5 else 17.0):
                dt = proximo_dia_8h + atraso
            else:
                return dt
        return dt

    def _escolher_tecnico(self, id_regiao: str, quando: datetime) -> dict | None:
        pool = [t for t in self.tec_por_regiao[id_regiao] if self._ativo(t, quando)]
        if not pool:
            pool = [t for t in self.tecnicos if self._ativo(t, quando)]
        if not pool:
            return None
        dia = quando.date()
        tec = min(random.sample(pool, min(3, len(pool))), key=lambda t: self.carga[(t["id_tecnico"], dia)])
        self.carga[(tec["id_tecnico"], dia)] += 1
        return tec

    def _sortear_assinante(self, quando: datetime, por_fragilidade: bool) -> int | None:
        for _ in range(8):
            reg = self._sortear_regiao()
            lista = self.ctos_por_regiao[reg["id_regiao"]]
            cto = (random.choices(lista, weights=self.peso_frag[reg["id_regiao"]])[0]
                   if por_fragilidade else random.choice(lista))
            cands = self.assin_por_cto[cto["id_cto"]]
            if not cands:
                continue
            for _ in range(4):
                idx = random.choice(cands)
                if self._ass_ativo(idx, quando):
                    return idx
        return None

    def _causa(self, tipo: str, cto: dict, improdutiva: bool) -> str:
        if improdutiva:
            causas = dict(CAUSAS_IMPROD)
            if tipo != "INST_FTTH":
                causas.pop("SEM_VIABILIDADE_TECNICA")
            return random.choices(list(causas), weights=list(causas.values()))[0]
        f = cto["_frag"]
        if tipo == "REP_FTTH":
            c = dict(CAUSAS_REP_FTTH)
            c["CTO_DEFEITO"] *= 1 + 4 * f
            c["DROP_ROMPIDO"] *= 1 + 2 * f
        elif tipo == "REP_LENT":
            c = dict(CAUSAS_REP_LENT)
            c["POTENCIA_BAIXA"] *= 1 + 4 * f
        elif tipo == "REP_ROMP":
            c = CAUSAS_ROMP
        else:
            return {"INST_FTTH": "INSTALADO_OK", "MUD_END": "MUDANCA_REALIZADA",
                    "MAN_PREV": "PREVENTIVA_REALIZADA",
                    "RET_EQP": "EQUIPAMENTO_NAO_DEVOLVIDO" if self.rng.random() < 0.10
                    else "EQUIPAMENTO_RETIRADO"}[tipo]
        return random.choices(list(c), weights=list(c.values()))[0]

    def _alarme(self, cto: dict, tipo: str, inicio: datetime, fim: datetime | None,
                id_assinante: str | None = None, qtd_onus: int = 1) -> dict:
        return {
            "id_olt": cto["id_olt"], "id_cto": cto["id_cto"], "porta_pon": cto["porta_pon"],
            "id_assinante": id_assinante, "tipo_alarme": tipo, "severidade": SEVERIDADE[tipo],
            "dt_inicio": inicio, "dt_fim": fim if (fim is not None and fim <= self.fim) else None,
            "potencia_rx_dbm": round(self._u(-29.5, -27.1), 2) if tipo == "LOW_RX_POWER" else None,
            "qtd_onus_afetadas": qtd_onus,
        }

    def gerar_ordens(self) -> pd.DataFrame:
        aberturas = self._aberturas()
        codigos = list(TIPOS_SERVICO)
        cache_pesos: dict = {}
        pendentes: list = []  # heap de reincidências agendadas: (data, idx_assinante)
        self.os_dia_regiao, self.carga = defaultdict(int), defaultdict(int)
        self.manut_cto = defaultdict(list)
        self.alarmes_os: list = []
        ordens = []

        for i, ab in enumerate(aberturas):
            if ab.month not in cache_pesos:
                cache_pesos[ab.month] = self._pesos_tipo(ab.month)
            tipo = random.choices(codigos, weights=cache_pesos[ab.month])[0]
            t = TIPOS_SERVICO[tipo]

            # quem / onde
            idx_ass = None
            if tipo in ("REP_FTTH", "REP_LENT"):
                while pendentes and pendentes[0][0] <= ab:
                    _, cand = heapq.heappop(pendentes)
                    if self._ass_ativo(cand, ab):
                        idx_ass = cand
                        break
                if idx_ass is None:
                    idx_ass = self._sortear_assinante(ab, por_fragilidade=True)
            elif tipo in ("MUD_END", "RET_EQP"):
                idx_ass = self._sortear_assinante(ab, por_fragilidade=False)

            if tipo == "INST_FTTH":
                reg = self._sortear_regiao()
                lista = self.ctos_por_regiao[reg["id_regiao"]]
                cto = random.choice(lista)
                for _ in range(5):  # prefere CTO com porta livre
                    if len(self.assin_por_cto[cto["id_cto"]]) < cto["capacidade_portas"]:
                        break
                    cto = random.choice(lista)
                cli = self._sortear_cliente()
                idx_ass = self._novo_assinante(cto, cli, None)
            elif idx_ass is not None:
                a = self.assinantes[idx_ass]
                cto, cli = a["_cto"], a["id_cliente"]
            else:  # MAN_PREV, REP_ROMP (sem assinante)
                reg = self._sortear_regiao()
                lista = self.ctos_por_regiao[reg["id_regiao"]]
                cto = (random.choices(lista, weights=self.peso_frag[reg["id_regiao"]])[0]
                       if tipo == "MAN_PREV" else random.choice(lista))
                cli = self._sortear_cliente()

            id_reg, dia = cto["id_regiao"], ab.date()
            fator = self._fator_carga(id_reg, dia)
            self.os_dia_regiao[(id_reg, dia)] += 1
            canal = ("PLANEJAMENTO" if tipo == "MAN_PREV" else "NOC" if tipo == "REP_ROMP"
                     else random.choices(CANAIS, weights=PESO_CANAIS)[0])
            os_ = {
                "id_os": i + 1, "numero_os": f"OS-{ab.year}-{i + 1:07d}", "id_cliente": cli,
                "codigo_tipo_servico": tipo,
                "id_assinante": self.assinantes[idx_ass]["id_assinante"] if idx_ass is not None else None,
                "id_cto": cto["id_cto"], "id_regiao": id_reg,
                "bairro": self.assinantes[idx_ass]["bairro"] if idx_ass is not None else cto["bairro"],
                "cidade": cto["cidade"], "uf": cto["uf"], "canal_abertura": canal,
                "id_tecnico": None, "status": None, "dt_abertura": ab, "dt_despacho": None,
                "dt_chegada": None, "dt_encerramento": None, "dt_cancelamento": None,
                "codigo_encerramento": None, "potencia_rx_dbm_final": None,
            }

            # cancelamento antes do atendimento
            r = self.rng.random()
            if r < t["p_cancel"]:
                dc = ab + timedelta(hours=self._u(0.5, 48))
                if dc <= self.fim:
                    os_.update(status="CANCELADA", dt_cancelamento=dc,
                               codigo_encerramento=random.choice(CODIGOS_CANCELAMENTO))
                else:
                    os_["status"] = "EM_ABERTO"
                ordens.append(os_)
                continue
            improdutiva = r < t["p_cancel"] + t["p_improd"]

            # tempos
            espera_h = self.rng.lognormal(math.log(t["espera_med_h"]), 0.6) * fator
            chegada = ab + timedelta(hours=float(espera_h))
            if tipo != "REP_ROMP":  # rompimento tem plantão 24h
                chegada = self._expediente(chegada)
            desloc_min = self.rng.lognormal(math.log(25 * self.regiao_por_id[id_reg]["fator_desloc"]), 0.4)
            despacho = max(ab + timedelta(minutes=3),
                           chegada - timedelta(minutes=float(desloc_min + self._u(5, 90))))
            tec = self._escolher_tecnico(id_reg, chegada)
            nivel = NIVEL_FATOR[tec["nivel"]] if tec else 1.0
            dur_min = (self._u(5, 25) if improdutiva
                       else t["dur_min"] * nivel * self.rng.lognormal(0, 0.35))
            encerramento = chegada + timedelta(minutes=float(dur_min))
            causa = self._causa(tipo, cto, improdutiva)
            id_tec = tec["id_tecnico"] if tec else None

            if despacho > self.fim:
                os_["status"] = "EM_ABERTO"
            elif encerramento > self.fim:
                os_.update(status="EM_ABERTO", id_tecnico=id_tec, dt_despacho=despacho,
                           dt_chegada=chegada if chegada <= self.fim else None)
            else:
                os_.update(status="IMPRODUTIVA" if improdutiva else "CONCLUIDA", id_tecnico=id_tec,
                           dt_despacho=despacho, dt_chegada=chegada, dt_encerramento=encerramento,
                           codigo_encerramento=causa)
                if not improdutiva:
                    self._efeitos_conclusao(tipo, t, cto, idx_ass, tec, causa, encerramento, os_, pendentes)

            # alarmes de rede que antecedem o chamado
            if tipo in ("REP_FTTH", "REP_LENT") and idx_ass is not None and not improdutiva:
                tipo_al = CAUSA_ALARME.get(causa)
                if tipo_al and self.rng.random() < 0.8:
                    ini = ab - timedelta(hours=self._u(0.3, 30))
                    self.alarmes_os.append(self._alarme(cto, tipo_al, ini, os_["dt_encerramento"],
                                                        self.assinantes[idx_ass]["id_assinante"]))
            elif tipo == "REP_ROMP":
                ini = ab - timedelta(minutes=self._u(5, 40))
                qtd = max(1, int(len(self.assin_por_cto[cto["id_cto"]]) * 0.9))
                self.alarmes_os.append(self._alarme(cto, "PON_DOWN", ini, os_["dt_encerramento"], None, qtd))

            ordens.append(os_)

        df = pd.DataFrame(ordens)
        cols_dt = ["dt_abertura", "dt_despacho", "dt_chegada", "dt_encerramento", "dt_cancelamento"]
        df["dt_atualizacao"] = df[cols_dt].apply(pd.to_datetime).max(axis=1)
        _fmt_datas(df, cols_dt + ["dt_atualizacao"])
        return df

    def _efeitos_conclusao(self, tipo, t, cto, idx_ass, tec, causa, encerramento, os_, pendentes):
        f = cto["_frag"]
        if t["grupo"] in ("ATIVACAO", "REPARO"):
            pot = self.rng.normal(-19.0 - 4 * f, 1.6) - (1.5 if causa == "SEM_DEFEITO_CONSTATADO" else 0)
            os_["potencia_rx_dbm_final"] = round(float(np.clip(pot, -29.5, -13.0)), 2)
        if tipo == "INST_FTTH":
            self.assinantes[idx_ass]["data_ativacao"] = encerramento
        elif tipo == "RET_EQP":
            self.assinantes[idx_ass]["data_cancelamento"] = encerramento
        if tipo in ("MAN_PREV", "REP_ROMP") or causa == "CTO_DEFEITO":
            self.manut_cto[cto["id_cto"]].append(encerramento)

        ruim = bool(tec and tec["_qualidade_ruim"])
        if tipo in ("REP_FTTH", "REP_LENT"):
            p = 0.04 + 0.22 * f + (0.08 if ruim else 0) + (0.10 if causa == "SEM_DEFEITO_CONSTATADO" else 0)
        elif tipo == "INST_FTTH":
            p = 0.02 + 0.10 * f + (0.07 if ruim else 0)
        else:
            return
        if self.rng.random() < p:
            dias = min(30.0, float(self.rng.lognormal(math.log(6), 0.8)))
            heapq.heappush(pendentes, (encerramento + timedelta(days=dias), idx_ass))

    # ---- alarmes e medições -----------------------------------------------------------------
    def gerar_alarmes(self) -> pd.DataFrame:
        linhas = list(self.alarmes_os)
        n_dias = len(self.dias)
        frag = np.array([c["_frag"] for c in self.ctos])
        chuva = np.array([1.5 if d.month in MESES_CHUVA else 1.0 for d in self.dias])

        # ruído de fundo por CTO/dia (CTOs frágeis alarmam mais)
        cont = self.rng.poisson((0.008 + 0.10 * frag)[:, None] * chuva[None, :])
        for ci, di in zip(*np.nonzero(cont)):
            cto = self.ctos[ci]
            cands = self.assin_por_cto[cto["id_cto"]]
            for _ in range(cont[ci, di]):
                tipo = random.choices(["LOW_RX_POWER", "HIGH_BER", "LOS"], weights=[0.45, 0.30, 0.25])[0]
                ini = self.dias[di].to_pydatetime() + timedelta(seconds=int(self.rng.integers(0, 86400)))
                dur = self.rng.lognormal(math.log(20 if tipo == "LOS" else 90), 0.9)
                id_ass = self.assinantes[random.choice(cands)]["id_assinante"] if cands else None
                linhas.append(self._alarme(cto, tipo, ini, ini + timedelta(minutes=float(dur)), id_ass))

        # quedas de energia: rajada de DYING_GASP em parte da região
        pesos_dia = chuva / chuva.sum()
        n_eventos = 0
        for r in REGIOES:
            lista = self.ctos_por_regiao[r["id_regiao"]]
            for _ in range(self.rng.poisson(1.3 * self.a.meses)):
                n_eventos += 1
                dia = self.dias[self.rng.choice(n_dias, p=pesos_dia)].to_pydatetime()
                centro = dia + timedelta(seconds=int(self.rng.integers(0, 86400)))
                dur = float(self.rng.lognormal(math.log(70), 0.7))
                for cto in random.sample(lista, max(1, int(len(lista) * self._u(0.15, 0.55)))):
                    ini = centro + timedelta(minutes=self._u(-10, 10))
                    qtd = max(1, int(len(self.assin_por_cto[cto["id_cto"]]) * self._u(0.4, 1.0)))
                    linhas.append(self._alarme(cto, "DYING_GASP", ini,
                                               ini + timedelta(minutes=dur * self._u(0.8, 1.2)), None, qtd))
        self.meta["gabarito"]["quedas_de_energia_simuladas"] = n_eventos

        df = pd.DataFrame(linhas)
        df = df[df["dt_inicio"] >= self.inicio].sort_values("dt_inicio").reset_index(drop=True)
        df.insert(0, "id_alarme", np.arange(1, len(df) + 1))
        df["qtd_onus_afetadas"] = df["qtd_onus_afetadas"].astype(int)
        _fmt_datas(df, ["dt_inicio", "dt_fim"])
        return df

    def gerar_medicoes(self) -> pd.DataFrame:
        n = len(self.dias)
        idx = np.arange(n)
        partes = []
        for cto in self.ctos:
            f = cto["_frag"]
            resets = sorted({(d.date() - self.inicio.date()).days for d in self.manut_cto[cto["id_cto"]]})
            marcos = np.array([-int(self.rng.integers(0, 120))] + resets)
            desde = idx - marcos[np.searchsorted(marcos, idx, side="right") - 1]
            media = self.rng.normal(-18.5, 1.1) - np.minimum(f * 0.07 * desde, 9.0) + self.rng.normal(0, 0.35, n)
            minima = media - np.abs(self.rng.normal(1.2 + 3 * f, 0.5, n))
            total = max(1, min(cto["capacidade_portas"], len(self.assin_por_cto[cto["id_cto"]])))
            p_off = np.clip(0.01 + 0.05 * f + 0.15 * (media < -25), 0, 0.9)
            partes.append(pd.DataFrame({
                "id_cto": cto["id_cto"], "data": self.dias.strftime("%Y-%m-%d"),
                "potencia_rx_media_dbm": media.round(2), "potencia_rx_min_dbm": minima.round(2),
                "onus_total": total, "onus_online": total - self.rng.binomial(total, p_off),
            }))
        return pd.concat(partes, ignore_index=True)

    # ---- sujeira ----------------------------------------------------------------------------
    def aplicar_sujeira(self, os_df: pd.DataFrame, ass_df: pd.DataFrame):
        s = self.a.sujeira
        if s <= 0:
            return os_df, ass_df
        cnt = self.meta["sujeira_injetada"]

        def amostra(df, frac, mascara=None):
            base = df.index if mascara is None else df.index[mascara]
            k = int(round(len(base) * frac))
            return self.rng.choice(base, size=k, replace=False) if k else np.array([], dtype=int)

        def baguncar(txt):
            if txt is None:
                return txt
            return random.choice([str.upper, str.lower, _sem_acento, lambda x: f"  {x} "])(txt)

        # OS: encerramento antes da chegada (campos trocados na origem)
        ix = amostra(os_df, s * 0.1, (os_df["status"] == "CONCLUIDA").values)
        os_df.loc[ix, ["dt_chegada", "dt_encerramento"]] = os_df.loc[ix, ["dt_encerramento", "dt_chegada"]].values
        cnt["os_encerramento_antes_da_chegada"] = len(ix)
        # OS: técnico nulo em OS concluída
        ix = amostra(os_df, s * 0.3, (os_df["status"] == "CONCLUIDA").values)
        os_df.loc[ix, "id_tecnico"] = None
        cnt["os_concluida_sem_tecnico"] = len(ix)
        # OS: bairro com caixa/acentos/espaços inconsistentes
        ix = amostra(os_df, s)
        os_df.loc[ix, "bairro"] = [baguncar(v) for v in os_df.loc[ix, "bairro"]]
        cnt["os_bairro_inconsistente"] = len(ix)
        # OS: código do serviço em minúsculo
        ix = amostra(os_df, s * 0.5)
        os_df.loc[ix, "codigo_tipo_servico"] = os_df.loc[ix, "codigo_tipo_servico"].str.lower()
        cnt["os_codigo_minusculo"] = len(ix)
        # OS: data de abertura no formato dd/mm/aaaa hh:mm
        ix = amostra(os_df, s * 0.5)
        os_df.loc[ix, "dt_abertura"] = pd.to_datetime(os_df.loc[ix, "dt_abertura"]).dt.strftime("%d/%m/%Y %H:%M")
        cnt["os_data_formato_br"] = len(ix)
        # OS: duplicatas (reprocessamento na extração)
        ix = amostra(os_df, s * 0.4)
        os_df = pd.concat([os_df, os_df.loc[ix]]).sort_values("id_os", kind="stable").reset_index(drop=True)
        cnt["os_linhas_duplicadas"] = len(ix)

        # Assinantes
        ix = amostra(ass_df, min(1.0, s * 3))
        ass_df.loc[ix, "cep"] = ass_df.loc[ix, "cep"].str.replace("-", "", regex=False)
        cnt["assinante_cep_sem_hifen"] = len(ix)
        ix = amostra(ass_df, s)
        ass_df.loc[ix, "nome"] = ass_df.loc[ix, "nome"].str.upper()
        cnt["assinante_nome_maiusculo"] = len(ix)
        ix = amostra(ass_df, s * 0.2)
        ass_df = pd.concat([ass_df, ass_df.loc[ix]]).sort_values("id_assinante", kind="stable").reset_index(drop=True)
        cnt["assinante_linhas_duplicadas"] = len(ix)
        return os_df, ass_df

    # ---- orquestração -----------------------------------------------------------------------
    def executar(self) -> dict[str, pd.DataFrame]:
        t0 = time.time()
        tabelas = {
            "dim_regiao": self.gerar_regioes(),
            "clientes_operadoras": self.gerar_clientes(),
            "tipos_servico": self.gerar_tipos(),
            "contratos_sla": self.gerar_contratos(),
            "tecnicos": self.gerar_tecnicos(),
            "ctos": self.gerar_ctos(),
        }
        print("dimensões prontas; gerando assinantes e ordens de serviço...")
        self.gerar_base_assinantes()
        os_df = self.gerar_ordens()
        print("gerando alarmes de rede...")
        tabelas["alarmes_rede"] = self.gerar_alarmes()
        if not self.a.sem_medicoes:
            print("gerando medições diárias de potência...")
            tabelas["medicoes_potencia"] = self.gerar_medicoes()
        os_df, ass_df = self.aplicar_sujeira(os_df, self.tabela_assinantes())
        tabelas["assinantes"] = ass_df
        tabelas["ordens_servico"] = os_df
        self.meta["periodo"] = {"inicio": self.inicio.date(), "fim_exclusivo": self.fim.date()}
        self.meta["linhas_por_tabela"] = {k: len(v) for k, v in tabelas.items()}
        self.meta["tempo_geracao_s"] = round(time.time() - t0, 1)
        return tabelas

    def salvar(self, tabelas: dict[str, pd.DataFrame]) -> Path:
        saida = Path(self.a.saida)
        saida.mkdir(parents=True, exist_ok=True)
        for nome, df in tabelas.items():
            if self.a.formato == "parquet":
                df.to_parquet(saida / f"{nome}.parquet", index=False)
            else:
                df.to_csv(saida / f"{nome}.csv", index=False, encoding="utf-8")
        with open(saida / "_metadados.json", "w", encoding="utf-8") as f:
            json.dump(self.meta, f, ensure_ascii=False, indent=2, default=str)
        return saida


def main() -> None:
    p = argparse.ArgumentParser(description="Gera dados sintéticos de operações de campo de telecom (FTTH).")
    p.add_argument("--saida", default="dados_telecom", help="pasta de saída")
    p.add_argument("--inicio", default="2025-09-01", help="data inicial (AAAA-MM-DD)")
    p.add_argument("--meses", type=int, default=12, help="quantidade de meses simulados")
    p.add_argument("--ordens", type=int, default=50_000, help="quantidade aproximada de ordens de serviço")
    p.add_argument("--assinantes", type=int, default=12_000, help="assinantes já ativos no início do período")
    p.add_argument("--ctos", type=int, default=1_200, help="quantidade de CTOs na rede")
    p.add_argument("--turnover", type=float, default=0.15, help="fração de técnicos que saem no período")
    p.add_argument("--sujeira", type=float, default=0.03, help="intensidade dos problemas de qualidade (0 a 0.2)")
    p.add_argument("--formato", choices=["csv", "parquet"], default="csv")
    p.add_argument("--sem-medicoes", action="store_true", help="não gerar medicoes_potencia")
    p.add_argument("--seed", type=int, default=42, help="semente (mesma semente = mesmos dados)")
    args = p.parse_args()

    gerador = GeradorTelecom(args)
    tabelas = gerador.executar()
    pasta = gerador.salvar(tabelas)

    print(f"\nDados salvos em: {pasta.resolve()}")
    for nome, n in gerador.meta["linhas_por_tabela"].items():
        print(f"  {nome:<22} {n:>10,} linhas".replace(",", "."))
    print(f"Tempo: {gerador.meta['tempo_geracao_s']}s  |  gabarito e sujeira em _metadados.json")


if __name__ == "__main__":
    main()
