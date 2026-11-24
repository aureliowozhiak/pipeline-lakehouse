from lakehouse.utils.naming import (
    bronze_column_names,
    dataset_name_from_file,
    normalize_column_names,
    to_snake_case,
)


def test_snake_case_remove_acentos_e_espacos():
    assert to_snake_case("Data de Cadastro") == "data_de_cadastro"
    assert to_snake_case("  Preço Unitário (R$) ") == "preco_unitario_r"
    assert to_snake_case("Observação") == "observacao"
    assert to_snake_case("E-mail") == "e_mail"


def test_snake_case_camel_case_e_numeros():
    assert to_snake_case("valorTotal") == "valor_total"
    assert to_snake_case("IDCliente") == "id_cliente"
    assert to_snake_case("pedidoId") == "pedido_id"
    assert to_snake_case("2024 vendas") == "col_2024_vendas"


def test_snake_case_bom_e_vazio():
    assert to_snake_case("\ufeffnome") == "nome"
    assert to_snake_case("!!!") == ""


def test_normalize_garante_nomes_unicos_e_nao_vazios():
    names = normalize_column_names(["Nome", "nome", "NOME ", "", "%%"])
    assert names == ["nome", "nome_2", "nome_3", "coluna_4", "coluna_5"]
    assert len(set(names)) == len(names)


def test_normalize_respeita_reservados():
    assert normalize_column_names(["x"], reserved=["x"]) == ["x_2"]


def test_bronze_preserva_nome_e_troca_so_caracteres_invalidos():
    assert bronze_column_names(["Data de Cadastro", "Preço (R$)", "a.b", "Nome", "nome"]) == [
        "Data_de_Cadastro", "Preço__R$_", "a_b", "Nome", "nome_2",
    ]


def test_nome_do_dataset():
    assert dataset_name_from_file("Vendas 2024 - Jan") == "vendas_2024_jan"
    assert dataset_name_from_file("@@@") == "dataset"
