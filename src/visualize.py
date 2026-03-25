"""
visualize.py — Módulo de Visualización para Paper Académico
=============================================================
Motor de gráficos de alta resolución (DPI=300) diseñado para
publicación científica. Carga los artefactos Parquet ya generados
y produce 8 figuras auditadas, con manejo seguro de datos vacíos.

Auditoría de bugs corregidos:
  - Drawdown: fórmula correcta (v_actual / max_historico_acumulado) - 1
  - Benchmark: rebasing estricto en la misma fecha de inicio del portafolio
  - Merge de fechas: se castea a datetime64[ns] antes de cruzar
  - Tipos mixtos: se convierte a float64 con errors='coerce' antes de pct_change()
  - Gráfico vacío: bloque if not df.empty antes de cada savefig()

Autor: Farit Alexander Reasco Torres — PUCESE (2026)
"""

import os
import logging
import warnings
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

matplotlib.use("Agg")   # Backend no interactivo; obligatorio para servidores/CI
warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ── Estilo global de publicación ──────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor":  "#FFFFFF",
    "axes.facecolor":    "#F8F8F8",
    "axes.edgecolor":    "#CCCCCC",
    "axes.grid":         True,
    "grid.color":        "#E0E0E0",
    "grid.linewidth":    0.6,
    "font.family":       "DejaVu Sans",
    "font.size":         9,
    "axes.titlesize":    10,
    "axes.labelsize":    9,
    "legend.fontsize":   8,
    "xtick.labelsize":   8,
    "ytick.labelsize":   8,
})

FIG_DIR   = "docs/data_analysis_in_Fintech"
DPI       = 300
PALETTE   = sns.color_palette("tab10")


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _save(fig: plt.Figure, nombre: str) -> None:
    """Guarda con DPI=300, bbox_inches='tight' y cierra la figura."""
    os.makedirs(FIG_DIR, exist_ok=True)
    ruta = os.path.join(FIG_DIR, nombre)
    fig.savefig(ruta, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    logging.info(f"Figura exportada: {ruta}")


def _load_parquet(nombre: str, out_dir: str = "data/out_parquet") -> pd.DataFrame:
    """Carga un Parquet y castea la columna 'fecha' a datetime."""
    ruta = os.path.join(out_dir, f"{nombre}.parquet")
    if not os.path.exists(ruta):
        logging.warning(f"Artefacto no encontrado: {ruta}")
        return pd.DataFrame()
    df = pd.read_parquet(ruta, engine="pyarrow")
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    return df


def _safe_float(series: pd.Series) -> pd.Series:
    """Convierte cualquier serie a float64 limpiando strings mixtos."""
    return pd.to_numeric(series, errors="coerce").astype(float)


# ─────────────────────────────────────────────────────────────────────────────
# FIG 1: Rendimiento acumulado vs Benchmark
# ─────────────────────────────────────────────────────────────────────────────

def fig_portafolio_vs_benchmark(cartera: pd.DataFrame, indices: pd.DataFrame) -> None:
    """
    BUG AUDITADO:
      - Se castean las fechas en ambos DataFrames antes del merge.
      - El benchmark se rebasa EXACTAMENTE en la primera fecha activa del portafolio
        (no en la primera fila del índice).
      - pct_change() se aplica sobre float64.
    """
    if cartera.empty:
        logging.error("fig1: cartera vacía — figura omitida.")
        return

    # Asegurar float
    cartera = cartera.copy()
    cartera["valor_cartera"] = _safe_float(cartera["valor_cartera"])
    cartera["fecha"] = pd.to_datetime(cartera["fecha"], errors="coerce")

    # Retorno portafolio agregado (todas las cuentas juntas)
    port = (
        cartera.groupby("fecha")["valor_cartera"]
        .sum()
        .sort_index()
    )
    # Primera fecha con valor real
    primera_fecha = port[port > 0].index.min()
    if pd.isna(primera_fecha):
        logging.error("fig1: portafolio sin valor positivo — figura omitida.")
        return

    port_retorno = port.pct_change().fillna(0.0).replace([np.inf, -np.inf], 0.0)
    port_acum = (1 + port_retorno).cumprod()
    # Rebasing desde fecha activa
    base = port_acum.loc[primera_fecha]
    port_acum = port_acum / base

    fig, ax = plt.subplots(figsize=(7.5, 3.5))
    ax.plot(port_acum.index, port_acum.values, lw=1.6, color=PALETTE[0], label="Portafolio")

    # Benchmark
    if not indices.empty and "fecha" in indices.columns:
        idx = indices.copy()
        idx["fecha"] = pd.to_datetime(idx["fecha"], errors="coerce")

        # Seleccionar primera columna de precio disponible
        precio_cols = [c for c in idx.columns if c not in ("fecha", "ticker", "nombre")]
        if not precio_cols and "cierre" in idx.columns:
            precio_cols = ["cierre"]

        for col in precio_cols[:1]:
            idx[col] = _safe_float(idx[col])
            bench = (
                idx.set_index("fecha")[col]
                .sort_index()
                .reindex(port.index)
                .ffill()
                .bfill()
            )
            bench_ret = bench.pct_change().fillna(0.0).replace([np.inf, -np.inf], 0.0)
            bench_acum = (1 + bench_ret).cumprod()
            # Rebasing en la MISMA fecha que el portafolio
            if primera_fecha in bench_acum.index:
                base_b = bench_acum.loc[primera_fecha]
            else:
                base_b = bench_acum.iloc[0]
            bench_acum = bench_acum / base_b
            ax.plot(bench_acum.index, bench_acum.values, lw=1.2,
                    color=PALETTE[1], linestyle="--", label=f"Índice ({col})")

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=0))
    ax.set_title("Rendimiento Acumulado: Portafolio vs. Índice de Referencia")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Rendimiento Acumulado (base = 1)")
    ax.legend()
    _save(fig, "fig_portafolio_vs_benchmark.png")


# ─────────────────────────────────────────────────────────────────────────────
# FIG 2: Exposición por sector (Donut)
# ─────────────────────────────────────────────────────────────────────────────

def fig_donut_sector(exp_sector: pd.DataFrame) -> None:
    if exp_sector.empty:
        logging.error("fig2: exposición_sector vacía — figura omitida.")
        return

    df = exp_sector.copy()
    df["peso"] = _safe_float(df["peso"])
    agg = df.groupby("sector")["peso"].mean().sort_values(ascending=False)
    agg = agg[agg > 0]

    if agg.empty:
        logging.error("fig2: todos los pesos son cero — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(5, 5))
    wedges, texts, autotexts = ax.pie(
        agg.values,
        labels=agg.index,
        autopct="%1.1f%%",
        startangle=140,
        wedgeprops=dict(width=0.55, edgecolor="white"),
        colors=sns.color_palette("tab10", len(agg)),
    )
    for at in autotexts:
        at.set_fontsize(7)
    ax.set_title("Exposición Promedio por Sector (Dona)")
    _save(fig, "fig_donut_sector.png")


# ─────────────────────────────────────────────────────────────────────────────
# FIG 3: Distribución de retornos diarios (Histograma)
# ─────────────────────────────────────────────────────────────────────────────

def fig_hist_retornos(cartera: pd.DataFrame) -> None:
    if cartera.empty:
        logging.error("fig3: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = _safe_float(df["retorno"])
    df = df.dropna(subset=["retorno"])
    df = df[np.isfinite(df["retorno"]) & (df["retorno"] != 0)]

    if df.empty:
        logging.error("fig3: sin retornos válidos — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(7, 3.5))
    sns.histplot(
        data=df, x="retorno", hue="cuenta_id",
        bins=40, kde=True, ax=ax,
        palette="tab10", alpha=0.6,
    )
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=1))
    ax.set_title("Distribución de Retornos Diarios por Cuenta")
    ax.set_xlabel("Retorno Diario")
    ax.set_ylabel("Frecuencia")
    _save(fig, "fig_hist_retornos.png")


# ─────────────────────────────────────────────────────────────────────────────
# FIG 4: Retornos por cuenta (Boxplots)
# ─────────────────────────────────────────────────────────────────────────────

def fig_box_retornos(cartera: pd.DataFrame) -> None:
    if cartera.empty:
        logging.error("fig4: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = _safe_float(df["retorno"])
    df = df.dropna(subset=["retorno"])
    df = df[np.isfinite(df["retorno"]) & (df["retorno"] != 0)]

    if df.empty:
        logging.error("fig4: sin retornos válidos — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(7, 3.5))
    cuentas = sorted(df["cuenta_id"].unique())
    sns.boxplot(
        data=df, x="cuenta_id", y="retorno",
        order=cuentas, palette="tab10", ax=ax,
        flierprops=dict(marker=".", markersize=3, alpha=0.4),
    )
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=1))
    ax.set_title("Distribución de Retornos Diarios por Cuenta (Boxplot)")
    ax.set_xlabel("Cuenta")
    ax.set_ylabel("Retorno Diario")
    _save(fig, "fig_box_retornos.png")


# ─────────────────────────────────────────────────────────────────────────────
# FIG 5: Volatilidad móvil 21 días (anualizada)
# ─────────────────────────────────────────────────────────────────────────────

def fig_vol_rolling(cartera: pd.DataFrame) -> None:
    if cartera.empty:
        logging.error("fig5: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = _safe_float(df["retorno"])
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    fig, ax = plt.subplots(figsize=(7.5, 3.5))
    ploteado = False

    for cta, g in df.groupby("cuenta_id"):
        g = g.sort_values("fecha").set_index("fecha")
        vol = (
            g["retorno"]
            .rolling(21, min_periods=10)
            .std()
            .mul(np.sqrt(252))
        )
        vol = vol.dropna()
        if vol.empty:
            continue
        ax.plot(vol.index, vol.values, lw=1.2, label=str(cta))
        ploteado = True

    if not ploteado:
        logging.error("fig5: sin datos de volatilidad — figura omitida.")
        plt.close(fig)
        return

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=0))
    ax.set_title("Volatilidad Móvil Anualizada — Ventana 21 Días")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Volatilidad Anualizada (σ × √252)")
    ax.legend(title="Cuenta")
    _save(fig, "fig_vol_rolling.png")


# ─────────────────────────────────────────────────────────────────────────────
# FIG 6: Retornos mensuales (heatmap de calendario)
# ─────────────────────────────────────────────────────────────────────────────

def fig_retornos_mensuales(cartera: pd.DataFrame) -> None:
    if cartera.empty:
        logging.error("fig6: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = _safe_float(df["retorno"])
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha", "retorno"])

    # Retorno agregado (todas las cuentas, por mes)
    df["anio_mes"] = df["fecha"].dt.to_period("M")
    mensual = (
        df.groupby("anio_mes")["retorno"]
        .apply(lambda r: (1 + r).prod() - 1)
        .reset_index()
    )
    mensual.columns = ["periodo", "retorno_mensual"]

    if mensual.empty or mensual["retorno_mensual"].isna().all():
        logging.error("fig6: sin retornos mensuales — figura omitida.")
        return

    mensual["anio"] = mensual["periodo"].dt.year
    mensual["mes"]  = mensual["periodo"].dt.month
    pivot = mensual.pivot(index="anio", columns="mes", values="retorno_mensual")
    MESES_ES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
    pivot.columns = [MESES_ES[m - 1] for m in pivot.columns]

    fig, ax = plt.subplots(figsize=(10, max(2.5, len(pivot) * 0.8)))
    sns.heatmap(
        pivot, annot=True, fmt=".1%", linewidths=0.5,
        cmap="RdYlGn", center=0, ax=ax,
        annot_kws={"size": 7},
    )
    ax.set_title("Rendimiento Mensual del Portafolio Agregado")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Año")
    _save(fig, "fig_retornos_mensuales.png")


# ─────────────────────────────────────────────────────────────────────────────
# FIG 7: Curva de Drawdown
# ─────────────────────────────────────────────────────────────────────────────

def fig_drawdown(cartera: pd.DataFrame) -> None:
    """
    BUG AUDITADO:
      - Fórmula: dd = (valor_actual / max_historico_acumulado) - 1
      - Si valor_cartera llega en 0, se ignora con where(port > 0).
      - El max acumulado se calcula con expanding().max() sobre la serie float.
    """
    if cartera.empty:
        logging.error("fig7: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["valor_cartera"] = _safe_float(df["valor_cartera"])
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    port = (
        df.groupby("fecha")["valor_cartera"]
        .sum()
        .sort_index()
        .astype(float)
    )
    port = port[port > 0]   # Eliminar fechas sin capital

    if port.empty:
        logging.error("fig7: portafolio sin valor positivo — figura omitida.")
        return

    max_acum = port.expanding().max()
    drawdown = (port / max_acum) - 1.0
    drawdown = drawdown.replace([np.inf, -np.inf], np.nan).dropna()

    if drawdown.empty or drawdown.isna().all():
        logging.error("fig7: drawdown sin datos válidos — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(7.5, 3.5))
    ax.fill_between(drawdown.index, drawdown.values, 0,
                    color=PALETTE[3], alpha=0.55, label="Drawdown")
    ax.plot(drawdown.index, drawdown.values, lw=0.8, color=PALETTE[3])
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=1))
    ax.set_title("Curva de Drawdown — Caída desde Máximos Históricos")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Drawdown (%)")
    ax.legend()
    _save(fig, "fig_drawdown.png")


# ─────────────────────────────────────────────────────────────────────────────
# FIG 8: Top-10 Tickers por valor promedio
# ─────────────────────────────────────────────────────────────────────────────

def fig_top10_tickers(exp_ticker: pd.DataFrame) -> None:
    """
    BUG AUDITADO:
      - Se agrega valor_promedio a nivel global (sin filtrar por cuenta_id)
        para evitar que las barras aparezcan vacías cuando hay una sola cuenta.
      - Se castea a float antes de ordenar.
    """
    if exp_ticker.empty:
        logging.error("fig8: exposición_ticker vacía — figura omitida.")
        return

    df = exp_ticker.copy()
    df["valor_promedio"] = _safe_float(df["valor_promedio"])
    df = df.dropna(subset=["valor_promedio"])
    df = df[df["valor_promedio"] > 0]

    if df.empty:
        logging.error("fig8: todos los valores son cero — figura omitida.")
        return

    top = (
        df.groupby("ticker")["valor_promedio"]
        .sum()
        .nlargest(10)
        .sort_values()
    )

    fig, ax = plt.subplots(figsize=(6.5, 4))
    colors = sns.color_palette("Blues_d", len(top))
    bars = ax.barh(top.index, top.values, color=colors, edgecolor="white")
    ax.bar_label(bars, labels=[f"${v:,.0f}" for v in top.values],
                 padding=4, fontsize=7)
    ax.set_title("Top 10 Tickers por Valor Promedio de Posición")
    ax.set_xlabel("Valor Promedio (USD)")
    ax.set_ylabel("Ticker")
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    _save(fig, "fig_top10_tickers.png")


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR DE VISUALIZACIONES
# ─────────────────────────────────────────────────────────────────────────────

def generate_all_figures(out_parquet: str = "data/out_parquet") -> None:
    """Carga todos los artefactos Parquet y dispara las 8 figuras."""
    logging.info("Iniciando generación de figuras para el paper científico...")

    cartera    = _load_parquet("cartera_diaria",    out_parquet)
    exp_ticker = _load_parquet("exposicion_ticker", out_parquet)
    exp_sector = _load_parquet("exposicion_sector", out_parquet)

    # La hoja de índices no pasa por el ETL → se ingesta directamente
    indices_path = "data/nuevo_dataset.xlsx"
    try:
        indices = pd.read_excel(indices_path, sheet_name="indices")
        indices.columns = [str(c).strip().lower() for c in indices.columns]
    except Exception as e:
        logging.warning(f"No se pudo cargar la hoja 'indices': {e}")
        indices = pd.DataFrame()

    # Diagnóstico rápido
    logging.info(f"cartera filas: {len(cartera)} | columnas: {list(cartera.columns)}")
    logging.info(f"exp_ticker filas: {len(exp_ticker)}")
    logging.info(f"exp_sector filas: {len(exp_sector)}")
    if not cartera.empty:
        logging.info(f"valor_cartera stats:\n{cartera['valor_cartera'].describe()}")

    fig_portafolio_vs_benchmark(cartera, indices)
    fig_donut_sector(exp_sector)
    fig_hist_retornos(cartera)
    fig_box_retornos(cartera)
    fig_vol_rolling(cartera)
    fig_retornos_mensuales(cartera)
    fig_drawdown(cartera)
    fig_top10_tickers(exp_ticker)

    logging.info("Generación de figuras completada.")


if __name__ == "__main__":
    generate_all_figures()
