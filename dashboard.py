"""Dashboard do lakehouse local (Streamlit + Plotly).

Uso:  streamlit run dashboard.py

Lê as tabelas Delta com o pacote `deltalake` (sem Spark). O cache é invalidado
automaticamente quando a versão das tabelas muda (nova execução do pipeline);
o botão "Recarregar dados" força a releitura.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from lakehouse.config.settings import load_settings
from lakehouse.constants import GOLD_TABLES, RESERVED_COLUMNS, SILVER_HAS_ISSUES, SILVER_ISSUES
from lakehouse.dashboard import data as dd

st.set_page_config(page_title="Lakehouse local", layout="wide")

SETTINGS = load_settings()

KIND_LABELS = {
    "numerico": "Numérico",
    "categorico": "Categórico",
    "texto": "Texto livre",
    "data_hora": "Data/hora",
    "booleano": "Booleano",
}
LAYER_LABELS = {"bronze": "Bronze (dado original)", "silver": "Silver (limpo e tipado)", "gold": "Gold (métricas)"}


# ----------------------------------------------------------------------------- cache
@st.cache_data(show_spinner=False)
def cached_gold(signature: tuple) -> dict[str, pd.DataFrame]:
    return dd.load_gold(SETTINGS)


@st.cache_data(show_spinner=False)
def cached_registry(signature: tuple) -> pd.DataFrame:
    return dd.load_registry(SETTINGS)


@st.cache_data(show_spinner=False)
def cached_runs(signature: tuple) -> pd.DataFrame:
    return dd.load_runs(SETTINGS)


@st.cache_data(show_spinner=False)
def cached_table(path: str, version: int | None, columns: tuple | None, limit: int | None) -> pd.DataFrame:
    return dd.read_table(Path(path), list(columns) if columns else None, limit)


@st.cache_data(show_spinner=False)
def cached_count(path: str, version: int | None) -> int:
    return dd.count_rows(Path(path))


def read_layer(layer: str, name: str, columns: list[str] | None = None, limit: int | None = None) -> pd.DataFrame:
    path = dd.layer_path(SETTINGS, layer, name)
    return cached_table(str(path), dd.table_version(path), tuple(columns) if columns else None, limit)


def fmt_int(value) -> str:
    try:
        return f"{int(value):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "-"


def fmt_pct(value) -> str:
    return "-" if value is None or pd.isna(value) else f"{value:.1f}%".replace(".", ",")


# ----------------------------------------------------------------------------- páginas
def page_overview(gold: dict[str, pd.DataFrame], registry: pd.DataFrame, runs: pd.DataFrame) -> None:
    st.header("Visão geral")
    summary = gold["datasets_summary"]
    ok_files = int((registry["status"] == "SUCESSO").sum()) if not registry.empty else 0
    failed_files = int((registry["status"] == "FALHA").sum()) if not registry.empty else 0
    c = st.columns(5)
    c[0].metric("Datasets", len(summary))
    c[1].metric("Arquivos processados", ok_files)
    c[2].metric("Arquivos com falha", failed_files)
    c[3].metric("Registros (Silver)", fmt_int(summary["row_count"].sum() if not summary.empty else 0))
    c[4].metric("Linhas duplicadas", fmt_int(summary["duplicate_rows"].sum() if not summary.empty else 0))

    if not runs.empty:
        last = runs.iloc[0]
        st.caption(
            f"Última execução: {last['started_at']:%d/%m/%Y %H:%M:%S} · modo {last['run_mode']} · "
            f"status {last['status']} · {last['files_processed']} processado(s), "
            f"{last['files_skipped']} ignorado(s), {last['files_failed']} com falha"
        )

    if summary.empty:
        st.info("Nenhum dataset na camada Gold ainda.")
        return

    st.subheader("Datasets")
    view = summary[[
        "dataset", "source_files", "source_formats", "row_count", "column_count", "numeric_columns",
        "categorical_columns", "text_columns", "datetime_columns", "boolean_columns", "duplicate_rows",
        "rows_with_issues", "completeness_pct", "last_ingestion",
    ]].rename(columns={
        "dataset": "Dataset", "source_files": "Arquivos", "source_formats": "Formato(s)", "row_count": "Registros",
        "column_count": "Colunas", "numeric_columns": "Numéricas", "categorical_columns": "Categóricas",
        "text_columns": "Texto", "datetime_columns": "Data/hora", "boolean_columns": "Booleanas",
        "duplicate_rows": "Duplicadas", "rows_with_issues": "Linhas c/ problemas",
        "completeness_pct": "Completude %", "last_ingestion": "Última ingestão",
    })
    st.dataframe(view, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(summary, x="dataset", y="row_count", text="row_count", title="Registros por dataset",
                     labels={"dataset": "Dataset", "row_count": "Registros"})
        st.plotly_chart(fig)
    with col2:
        melted = summary.melt(
            id_vars="dataset",
            value_vars=["numeric_columns", "categorical_columns", "text_columns", "datetime_columns",
                        "boolean_columns"],
            var_name="tipo", value_name="colunas",
        )
        melted["tipo"] = melted["tipo"].map({
            "numeric_columns": "Numérico", "categorical_columns": "Categórico", "text_columns": "Texto livre",
            "datetime_columns": "Data/hora", "boolean_columns": "Booleano",
        })
        fig = px.bar(melted, x="dataset", y="colunas", color="tipo", title="Colunas por tipo",
                     labels={"dataset": "Dataset", "colunas": "Colunas", "tipo": "Tipo"})
        st.plotly_chart(fig)

    fig = px.bar(summary, x="dataset", y=["duplicate_pct", "rows_with_issues_pct"], barmode="group",
                 title="% de linhas duplicadas e com problemas",
                 labels={"value": "%", "dataset": "Dataset", "variable": "Métrica"})
    fig.for_each_trace(lambda t: t.update(name={"duplicate_pct": "Duplicadas",
                                                 "rows_with_issues_pct": "Com problemas"}[t.name]))
    st.plotly_chart(fig)


def page_files(registry: pd.DataFrame, runs: pd.DataFrame, gold: dict[str, pd.DataFrame]) -> None:
    st.header("Arquivos processados")
    if registry.empty:
        st.info("Nenhum arquivo registrado ainda.")
    else:
        view = registry[[
            "file_key", "file_format", "dataset", "status", "action", "row_count", "column_count",
            "corrupt_records", "file_size_bytes", "processed_at", "batch_id", "read_options", "warnings",
            "error_message", "file_hash",
        ]].rename(columns={
            "file_key": "Arquivo", "file_format": "Formato", "dataset": "Dataset", "status": "Status",
            "action": "Motivo", "row_count": "Registros", "column_count": "Colunas",
            "corrupt_records": "Malformados", "file_size_bytes": "Bytes", "processed_at": "Processado em",
            "batch_id": "Lote", "read_options": "Opções de leitura", "warnings": "Avisos",
            "error_message": "Erro", "file_hash": "SHA-256",
        })
        st.dataframe(view, hide_index=True)
        failed = registry[registry["status"] == "FALHA"]
        for _, row in failed.iterrows():
            st.error(f"**{row['file_key']}**: {row['error_message']}")

    rc = gold["record_counts"]
    if not rc.empty:
        st.subheader("Registros por arquivo de origem")
        fig = px.bar(rc, x="source_file", y="row_count", color="dataset", text="row_count",
                     labels={"source_file": "Arquivo", "row_count": "Registros", "dataset": "Dataset"})
        st.plotly_chart(fig)

    if not runs.empty:
        st.subheader("Histórico de execuções")
        st.dataframe(runs, hide_index=True)


def _numeric_charts(ds: str, col: str, hist: pd.DataFrame, prof_row: pd.Series) -> None:
    h = hist[(hist["dataset"] == ds) & (hist["column"] == col)].sort_values("bin_index")
    c1, c2 = st.columns(2)
    with c1:
        if not h.empty:
            labels = [f"{a:,.4g} a {b:,.4g}" for a, b in zip(h["bin_start"], h["bin_end"])]
            fig = go.Figure(go.Bar(x=(h["bin_start"] + h["bin_end"]) / 2, y=h["count"],
                                   width=(h["bin_end"] - h["bin_start"]).clip(lower=1e-9),
                                   customdata=labels, hovertemplate="%{customdata}<br>%{y} registros<extra></extra>"))
            fig.update_layout(title=f"Histograma de {col}", xaxis_title=col, yaxis_title="Registros", bargap=0.02)
            st.plotly_chart(fig, key=f"hist_{ds}_{col}")
    with c2:
        sample = read_layer("silver", ds, columns=[col], limit=200_000)
        if not sample.empty and sample[col].notna().any():
            values = pd.DataFrame({col: pd.to_numeric(sample[col], errors="coerce").astype("float64")})
            fig = px.box(values, y=col, points="outliers", title=f"Box plot de {col}")
            st.plotly_chart(fig, key=f"box_{ds}_{col}")
    st.caption(
        f"mín {prof_row['min_value']} · máx {prof_row['max_value']} · média {prof_row['mean']:.4g} · "
        f"mediana {prof_row['median']:.4g} · desvio {prof_row['stddev'] if pd.notna(prof_row['stddev']) else 0:.4g}"
        if pd.notna(prof_row["mean"]) else "Sem valores numéricos."
    )


def _bar_top_values(ds: str, col: str, freq: pd.DataFrame, title: str) -> None:
    f = freq[(freq["dataset"] == ds) & (freq["column"] == col)].sort_values("rank")
    if f.empty:
        st.caption("Sem valores não nulos.")
        return
    fig = px.bar(f, x="count", y="value", orientation="h", text="count", title=title,
                 labels={"count": "Registros", "value": col}, hover_data={"pct": ":.2f"})
    fig.update_layout(yaxis={"categoryorder": "total ascending", "type": "category"},
                      height=max(300, 28 * len(f) + 120))
    st.plotly_chart(fig, key=f"bar_{ds}_{col}")


def _datetime_chart(ds: str, col: str) -> None:
    sample = read_layer("silver", ds, columns=[col], limit=200_000)
    values = pd.to_datetime(sample[col], errors="coerce").dropna()
    if values.empty:
        st.caption("Sem valores de data/hora.")
        return
    span_days = (values.max() - values.min()).days
    freq = "D" if span_days <= 120 else "W" if span_days <= 730 else "MS"
    counts = values.dt.to_period({"D": "D", "W": "W", "MS": "M"}[freq]).dt.start_time.value_counts().sort_index()
    fig = px.bar(x=counts.index, y=counts.values, title=f"Registros ao longo do tempo ({col})",
                 labels={"x": col, "y": "Registros"})
    st.plotly_chart(fig, key=f"dt_{ds}_{col}")


def page_dataset(gold: dict[str, pd.DataFrame]) -> None:
    st.header("Explorar dataset")
    summary, profile = gold["datasets_summary"], gold["column_profile"]
    if summary.empty:
        st.info("Nenhum dataset disponível.")
        return
    ds = st.selectbox("Dataset", summary["dataset"].tolist(), key="ds_explore")
    s = summary[summary["dataset"] == ds].iloc[0]
    prof = profile[profile["dataset"] == ds].sort_values("position")

    c = st.columns(6)
    c[0].metric("Registros", fmt_int(s["row_count"]))
    c[1].metric("Colunas", int(s["column_count"]))
    c[2].metric("Duplicadas", fmt_int(s["duplicate_rows"]), help="Linhas marcadas com _is_duplicate = true")
    c[3].metric("Linhas c/ problemas", fmt_int(s["rows_with_issues"]))
    c[4].metric("Completude", fmt_pct(s["completeness_pct"]), help="% de células não nulas")
    c[5].metric("Arquivos", int(s["source_files"]))
    st.caption(f"Arquivo(s) de origem: {s['source_file_names']}")

    st.subheader("Colunas e tipos")
    view = prof[[
        "position", "column", "original_column", "data_type", "kind", "inference_rule", "inference_rate",
        "non_null_count", "null_count", "null_pct", "distinct_count", "min_value", "max_value",
    ]].copy()
    view["kind"] = view["kind"].map(KIND_LABELS)
    view["inference_rate"] = (view["inference_rate"] * 100).round(2)
    st.dataframe(view.rename(columns={
        "position": "#", "column": "Coluna (Silver)", "original_column": "Nome original", "data_type": "Tipo",
        "kind": "Categoria", "inference_rule": "Regra de inferência", "inference_rate": "% convertido",
        "non_null_count": "Não nulos", "null_count": "Nulos", "null_pct": "% nulos",
        "distinct_count": "Distintos", "min_value": "Mínimo", "max_value": "Máximo",
    }), hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        fig = px.bar(prof, x="column", y="null_pct", title="% de nulos por coluna",
                     labels={"column": "Coluna", "null_pct": "% nulos"}, range_y=[0, 100])
        st.plotly_chart(fig)
    with c2:
        kinds = prof["kind"].map(KIND_LABELS).value_counts().reset_index()
        kinds.columns = ["Categoria", "Colunas"]
        fig = px.pie(kinds, names="Categoria", values="Colunas", title="Categorias de colunas", hole=0.4)
        st.plotly_chart(fig)

    numeric = prof[prof["kind"] == "numerico"]
    if not numeric.empty:
        st.subheader("Estatísticas numéricas")
        st.dataframe(numeric[["column", "data_type", "non_null_count", "min_value", "max_value", "mean", "stddev",
                              "p25", "median", "p75"]].rename(columns={
            "column": "Coluna", "data_type": "Tipo", "non_null_count": "Não nulos", "min_value": "Mínimo",
            "max_value": "Máximo", "mean": "Média", "stddev": "Desvio padrão", "p25": "P25", "median": "Mediana",
            "p75": "P75"}), hide_index=True)

    texts = prof[prof["data_type"] == "string"]
    if not texts.empty:
        with st.expander("Tamanho dos textos"):
            st.dataframe(texts[["column", "min_length", "max_length", "avg_length", "distinct_count"]].rename(
                columns={"column": "Coluna", "min_length": "Mín.", "max_length": "Máx.", "avg_length": "Médio",
                         "distinct_count": "Distintos"}), hide_index=True)

    st.subheader("Gráficos por coluna")
    all_cols = prof["column"].tolist()
    default = all_cols[: min(4, len(all_cols))]
    chosen = st.multiselect("Colunas para visualizar", all_cols, default=default, key=f"cols_{ds}")
    for col in chosen:
        row = prof[prof["column"] == col].iloc[0]
        st.markdown(f"**{col}** · {row['data_type']} · {KIND_LABELS.get(row['kind'], row['kind'])} · "
                    f"{fmt_pct(row['null_pct'])} nulos · {fmt_int(row['distinct_count'])} distintos")
        if row["kind"] == "numerico":
            _numeric_charts(ds, col, gold["numeric_histograms"], row)
            if row["distinct_count"] <= SETTINGS.gold.get("categorical_max_distinct", 50):
                with st.expander(f"Frequência dos valores de {col}"):
                    _bar_top_values(ds, col, gold["value_frequencies"], f"Valores mais frequentes de {col}")
        elif row["kind"] == "data_hora":
            _datetime_chart(ds, col)
        else:
            title = f"Valores mais frequentes de {col}" + (" (texto livre: top N)" if row["kind"] == "texto" else "")
            _bar_top_values(ds, col, gold["value_frequencies"], title)
        st.divider()


def page_quality(gold: dict[str, pd.DataFrame]) -> None:
    st.header("Qualidade dos dados")
    qm = gold["quality_metrics"]
    if qm.empty:
        st.info("Sem métricas de qualidade.")
        return
    ds = st.selectbox("Dataset", sorted(qm["dataset"].unique()), key="ds_quality")
    q = qm[qm["dataset"] == ds]
    rows = q[q["column"] == "(linha)"]
    c = st.columns(len(rows))
    for i, (_, r) in enumerate(rows.iterrows()):
        c[i].metric(r["metric"].replace("_", " ").capitalize(), fmt_int(r["count"]), help=r["description"],
                    delta=fmt_pct(r["pct"]), delta_color="off")

    cols = q[q["column"] != "(linha)"]
    pivot = cols.pivot_table(index="column", columns="metric", values="count", aggfunc="sum").fillna(0)
    st.subheader("Problemas por coluna")
    fig = px.imshow(pivot, text_auto=True, aspect="auto", color_continuous_scale="Reds",
                    labels={"x": "Métrica", "y": "Coluna", "color": "Ocorrências"})
    fig.update_layout(height=max(320, 30 * len(pivot) + 120))
    st.plotly_chart(fig)
    with st.expander("Descrição das métricas"):
        st.dataframe(q[["metric", "description"]].drop_duplicates(), hide_index=True)

    st.subheader("Linhas com problemas (Silver)")
    sample = read_layer("silver", ds, limit=None)
    if SILVER_HAS_ISSUES in sample.columns:
        bad = sample[sample[SILVER_HAS_ISSUES]]
        st.caption(f"{len(bad)} linha(s) com pelo menos um problema. Nada foi removido: as linhas continuam na Silver.")
        front = [SILVER_ISSUES] + [c for c in bad.columns if c not in RESERVED_COLUMNS] + ["_source_file"]
        st.dataframe(bad[front].head(500), hide_index=True)


def page_layers() -> None:
    st.header("Prévia das camadas Bronze, Silver e Gold")
    layer = st.radio("Camada", ["bronze", "silver", "gold"], format_func=LAYER_LABELS.get, horizontal=True)
    names = dd.list_datasets(SETTINGS, layer)
    if not names:
        st.info(f"Camada {layer} vazia.")
        return
    label = "Tabela" if layer == "gold" else "Dataset"
    name = st.selectbox(label, names, key=f"tbl_{layer}")
    path = dd.layer_path(SETTINGS, layer, name)
    version = dd.table_version(path)
    schema = dd.table_schema(path)
    total = cached_count(str(path), version)
    c = st.columns(3)
    c[0].metric("Registros", fmt_int(total))
    c[1].metric("Colunas", len(schema))
    c[2].metric("Versão Delta", version)
    st.caption(f"Caminho: {path}")

    all_cols = [n for n, _ in schema]
    cols = st.multiselect("Colunas", all_cols, default=all_cols, key=f"pcols_{layer}_{name}")
    limit = st.slider("Linhas na prévia", 10, 1000, 100, step=10, key=f"lim_{layer}")
    df = read_layer(layer, name, columns=cols or None, limit=limit)
    if layer == "gold" and "dataset" in df.columns:
        options = sorted(df["dataset"].dropna().unique())
        sel = st.multiselect("Filtrar datasets", options, default=options, key=f"gds_{name}")
        df = df[df["dataset"].isin(sel)]
    st.dataframe(df, hide_index=True)
    with st.expander("Schema"):
        st.dataframe(pd.DataFrame(schema, columns=["Coluna", "Tipo (Arrow)"]), hide_index=True)


# ----------------------------------------------------------------------------- main
def main() -> None:
    st.sidebar.title("Lakehouse local")
    st.sidebar.caption("Spark + Delta Lake · Bronze → Silver → Gold")
    if st.sidebar.button("Recarregar dados", type="primary"):
        st.cache_data.clear()
        st.rerun()

    if not dd.is_delta(dd.control_path(SETTINGS, "file_registry")):
        st.title("Lakehouse local")
        st.warning(
            "Ainda não há dados no lakehouse. Coloque arquivos em `data/input/` (ou rode "
            "`python generate_sample_data.py`) e execute `python pipeline.py`. Depois clique em **Recarregar dados**."
        )
        return

    signature = dd.lakehouse_signature(SETTINGS)
    gold = cached_gold(signature)
    for table in GOLD_TABLES:
        gold.setdefault(table, pd.DataFrame())
    registry = cached_registry(signature)
    runs = cached_runs(signature)

    page = st.sidebar.radio("Página", ["Visão geral", "Explorar dataset", "Qualidade", "Arquivos processados",
                                       "Camadas (Bronze/Silver/Gold)"])
    st.sidebar.caption(f"Lakehouse: {SETTINGS.lakehouse_dir}")

    if page == "Visão geral":
        page_overview(gold, registry, runs)
    elif page == "Explorar dataset":
        page_dataset(gold)
    elif page == "Qualidade":
        page_quality(gold)
    elif page == "Arquivos processados":
        page_files(registry, runs, gold)
    else:
        page_layers()


main()
