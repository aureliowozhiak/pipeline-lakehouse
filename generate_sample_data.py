"""Gera arquivos de exemplo em data/input/ (CSV, JSON array, JSON Lines, TXT delimitado e TXT texto livre).

Os dados são fictícios e incluem, de propósito: nulos em vários formatos ("", NULL, N/A),
espaços sobrando, linhas duplicadas, delimitadores diferentes de vírgula (| e ;), decimais
no padrão brasileiro, datas em formatos diferentes, objetos aninhados, um arquivo em
Windows-1252 e alguns valores inválidos/malformados.

Uso:  python generate_sample_data.py
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from lakehouse.config.settings import load_settings

SEED = 42
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Hugo", "Isabela", "João",
         "Larissa", "Marcos", "Natália", "Otávio", "Paula", "Rafael", "Sofia", "Thiago", "Vitória", "William"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Lima", "Costa", "Ribeiro", "Almeida", "Gomes"]
CIDADES = [("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"), ("Curitiba", "PR"),
           ("Porto Alegre", "RS"), ("Salvador", "BA"), ("Recife", "PE"), ("Fortaleza", "CE"), ("Goiânia", "GO")]
PRODUTOS = [("Notebook", "Eletrônicos", 3500.0), ("Mouse", "Acessórios", 89.9), ("Teclado", "Acessórios", 199.9),
            ("Monitor", "Eletrônicos", 1299.0), ("Cadeira", "Móveis", 899.0), ("Mesa", "Móveis", 1150.0),
            ("Headset", "Acessórios", 349.5), ("Webcam", "Eletrônicos", 279.0)]
CANAIS = ["loja", "site", "app", "telefone"]


def br_number(value: float) -> str:
    """1234.5 -> '1.234,50'"""
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def gen_clientes(rng: random.Random, path: Path) -> int:
    header = ["ID Cliente", "Código", "Nome Completo", "E-mail", "Idade", "Cidade", "UF",
              "Data de Cadastro", "Ativo", "Saldo (R$)", "Observação"]
    rows = []
    for i in range(1, 121):
        nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
        cidade, uf = rng.choice(CIDADES)
        cadastro = datetime(2022, 1, 1) + timedelta(days=rng.randint(0, 900))
        idade: str = str(rng.randint(18, 80))
        if i % 37 == 0:
            idade = "trinta e dois"  # valor inválido: a coluna ainda vira inteiro (>95% ok)
        elif i % 19 == 0:
            idade = rng.choice(["", "NULL", "N/A"])
        email = f"{nome.split()[0].lower()}.{i}@exemplo.com" if i % 11 else ""
        rows.append([
            str(i),
            f"{i:06d}",  # zeros à esquerda: deve continuar texto
            f"  {nome}  " if i % 9 == 0 else nome,  # espaços sobrando
            email,
            idade,
            f" {cidade} " if i % 7 == 0 else cidade,
            uf,
            cadastro.strftime("%d/%m/%Y"),
            rng.choice(["Sim", "Não", "sim", "NÃO"]),
            f"{rng.uniform(-500, 15000):.2f}" if i % 13 else "null",
            rng.choice(["", "", "", "Cliente VIP, atendimento prioritário", "Prefere contato por e-mail"]),
        ])
    rows += [rows[4], rows[10], rows[10], rows[50]]  # duplicados exatos
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    return len(rows)


def gen_vendas_txt(rng: random.Random, path: Path) -> int:
    header = ["id_venda", "data_venda", "produto", "categoria", "quantidade", "valor_unitario", "desconto_pct", "canal"]
    rows = []
    for i in range(1, 201):
        produto, categoria, preco = rng.choice(PRODUTOS)
        data = datetime(2024, 1, 1) + timedelta(days=rng.randint(0, 364))
        rows.append([
            f"V{i:05d}",
            data.strftime("%Y-%m-%d"),
            produto,
            categoria if i % 17 else "",
            str(rng.randint(1, 10)),
            br_number(preco * rng.uniform(0.9, 1.1)),  # "1.234,56"
            br_number(rng.choice([0, 0, 5, 10, 15])) if i % 23 else "-",
            rng.choice(CANAIS),
        ])
    rows += [rows[3], rows[99]]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="|", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return len(rows)


def gen_pedidos_json(rng: random.Random, path: Path) -> int:
    pedidos = []
    for i in range(1, 61):
        nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
        cidade, uf = rng.choice(CIDADES)
        itens = [{"produto": p[0], "qtd": rng.randint(1, 3)} for p in rng.sample(PRODUTOS, rng.randint(1, 3))]
        criado = datetime(2024, 3, 1, 8) + timedelta(minutes=rng.randint(0, 60 * 24 * 90))
        pedido = {
            "pedidoId": 1000 + i,
            "criadoEm": criado.strftime("%Y-%m-%dT%H:%M:%S"),
            "cliente": {"nome": nome, "endereco": {"cidade": cidade, "uf": uf}},
            "itens": itens,
            "valorTotal": round(rng.uniform(50, 5000), 2),
            "pago": rng.random() > 0.2,
            "cupom": rng.choice([None, None, "PROMO10", "FRETEGRATIS"]),
        }
        if i % 15 == 0:
            del pedido["cupom"]  # chave ausente em alguns registros
        pedidos.append(pedido)
    pedidos.append(dict(pedidos[7]))  # duplicado
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(pedidos, fh, ensure_ascii=False, indent=2)
    return len(pedidos)


def gen_sensores_jsonl(rng: random.Random, path: Path) -> int:
    lines = []
    inicio = datetime(2024, 6, 1)
    for i in range(300):
        rec = {
            "sensor": f"S-{rng.randint(1, 6):02d}",
            "leitura_em": (inicio + timedelta(minutes=10 * i)).strftime("%Y-%m-%d %H:%M:%S"),
            "temperatura": round(rng.gauss(24, 4), 2),
            "umidade": round(rng.uniform(30, 90), 1),
            "status": rng.choice(["ok", "ok", "ok", "alerta", "falha"]),
        }
        if i % 25 == 0:
            rec["temperatura"] = None
        if i % 40 == 0:
            del rec["umidade"]
        lines.append(json.dumps(rec, ensure_ascii=False))
    lines += [lines[10], lines[11]]
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")
    return len(lines)


def gen_estoque_csv(rng: random.Random, path: Path) -> int:
    """CSV no estilo "Excel brasileiro": ';' como separador e codificação Windows-1252."""
    itens = ["Café torrado 500g", "Açúcar refinado 1kg", "Feijão carioca 1kg", "Pão de queijo 400g",
             "Óleo de soja 900ml", "Macarrão espaguete", "Leite integral 1L", "Arroz agulhinha 5kg"]
    lines = ["Código do Produto;Descrição;Preço Unitário;Estoque Mínimo;Última Atualização;Disponível"]
    for i in range(1, 41):
        atualizado = datetime(2024, 3, 1, 8) + timedelta(hours=rng.randint(0, 24 * 60))
        lines.append(";".join([
            f"{i:05d}",
            rng.choice(itens),
            f"  {br_number(rng.uniform(2, 60))} " if i % 6 == 0 else br_number(rng.uniform(2, 60)),
            "" if i % 9 == 0 else ("NULL" if i % 14 == 0 else str(rng.randint(0, 50))),
            atualizado.strftime("%d/%m/%Y %H:%M"),
            rng.choice(["S", "N"]),
        ]))
    lines.append("00099;Linha com colunas a mais;1,00;2;09/03/2024 08:00;S;extra;extra")  # malformada
    lines.append(lines[3])  # duplicada
    with open(path, "w", encoding="cp1252", newline="") as fh:
        fh.write("\r\n".join(lines) + "\r\n")
    return len(lines) - 1


def gen_log_txt(rng: random.Random, path: Path) -> int:
    msgs = ["Serviço iniciado", "Conexão com o banco estabelecida", "Requisição recebida em /api/status",
            "Tempo de resposta acima do esperado", "Usuário autenticado", "Falha ao enviar e-mail, nova tentativa",
            "Cache atualizado", "Processo finalizado"]
    levels = ["INFO", "INFO", "INFO", "WARN", "ERROR", "DEBUG"]
    lines = []
    t = datetime(2024, 7, 1, 8)
    for i in range(150):
        t += timedelta(seconds=rng.randint(1, 120))
        lines.append(f"{t:%Y-%m-%d %H:%M:%S} {rng.choice(levels)} {rng.choice(msgs)}")
        if i % 30 == 29:
            lines.append("")  # linha em branco
    lines += ["   Linha com espaços nas bordas   ", lines[5]]
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")
    return len(lines)


def main() -> None:
    settings = load_settings()
    out = settings.input_dir
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    generated = [
        ("clientes.csv", gen_clientes, "CSV com vírgula, nulos variados, espaços e duplicados"),
        ("vendas.txt", gen_vendas_txt, "TXT delimitado por '|' com decimais no padrão BR"),
        ("pedidos.json", gen_pedidos_json, "JSON array (multilinha) com objetos aninhados"),
        ("sensores.json", gen_sensores_jsonl, "JSON Lines (um objeto por linha)"),
        ("log_sistema.txt", gen_log_txt, "TXT de texto livre (uma linha = um registro)"),
        ("estoque_filial.csv", gen_estoque_csv, "CSV com ';', Windows-1252 e uma linha malformada"),
    ]
    print(f"Gerando arquivos de exemplo em {out}")
    for name, func, desc in generated:
        n = func(rng, out / name)
        print(f"  {name:<18} {n:>4} registros  - {desc}")
    print("\nPróximo passo: python pipeline.py")


if __name__ == "__main__":
    main()
