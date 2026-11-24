import codecs

from lakehouse.ingestion.detection import (
    decode_sample,
    detect_delimiter,
    detect_encoding,
    detect_json_multiline,
    detect_txt_mode,
    first_record,
)


def test_detecta_virgula():
    text = "a,b,c\n1,2,3\n4,5,6\n"
    assert detect_delimiter(text).delimiter == ","


def test_detecta_ponto_e_virgula_com_decimais_br():
    text = "id;valor;desconto\n1;10,50;0,5\n2;3,99;1,0\n3;7,00;0,0\n"
    guess = detect_delimiter(text)
    assert guess.delimiter == ";"
    assert guess.columns == 3


def test_detecta_tab_e_pipe():
    assert detect_delimiter("a\tb\n1\t2\n3\t4\n").delimiter == "\t"
    assert detect_delimiter("a|b|c\n1|2|3\n").delimiter == "|"


def test_virgula_entre_aspas_nao_confunde():
    text = 'nome;obs\n"Silva, Ana";"ok, tudo bem"\n"Souza, Bia";"sem obs"\n'
    assert detect_delimiter(text).delimiter == ";"


def test_txt_texto_livre_vs_delimitado():
    log = "2024-01-01 10:00 INFO iniciado\n2024-01-01 10:01 WARN lento, verificar\n2024-01-01 10:02 INFO ok\n"
    assert detect_txt_mode(log)[0] == "plain"
    tabela = "col1|col2\nx|y\nz|w\n"
    mode, guess = detect_txt_mode(tabela)
    assert mode == "delimited" and guess.delimiter == "|"


def test_txt_com_delimitador_configurado():
    mode, guess = detect_txt_mode("a#b\n1#2\n", delimiter="#")
    assert mode == "delimited" and guess.delimiter == "#"


def test_json_array_vs_json_lines():
    assert detect_json_multiline('[{"a": 1}, {"a": 2}]') is True
    assert detect_json_multiline('{\n  "a": 1\n}\n') is True
    assert detect_json_multiline('{"a": 1}\n{"a": 2}\n') is False


def test_deteccao_de_codificacao():
    assert detect_encoding("ação".encode("utf-8")).spark == "UTF-8"
    assert detect_encoding("ação".encode("cp1252")).spark == "windows-1252"
    bom = detect_encoding(codecs.BOM_UTF8 + b"a,b")
    assert bom.has_bom and bom.spark == "UTF-8"
    assert detect_encoding(b"abc", "latin-1").spark == "ISO-8859-1"


def test_amostra_truncada_descarta_ultima_linha_e_bom():
    enc = detect_encoding(codecs.BOM_UTF8 + b"x")
    text = decode_sample(codecs.BOM_UTF8 + b"a,b\n1,2\n3,", enc, truncated=True)
    assert text == "a,b\n1,2"
    assert first_record(text, ",") == ["a", "b"]
