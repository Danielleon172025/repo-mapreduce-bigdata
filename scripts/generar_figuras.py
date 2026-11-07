import os
import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

REPO = sys.argv[1]
RES = os.path.join(REPO, "resultados")
OUT = os.path.join(RES, "figuras")
os.makedirs(OUT, exist_ok=True)

GRIS = "#9a9a9a"
AZUL = "#2f4f8f"
TINTA = "#222222"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": "#bbbbbb",
                     "axes.labelcolor": TINTA, "xtick.color": TINTA, "ytick.color": TINTA})


def leer(nombre):
    filas = []
    with open(os.path.join(RES, nombre), encoding="utf-8") as f:
        for linea in f:
            linea = linea.rstrip("\n")
            if not linea:
                continue
            k, v = linea.split("\t")
            total, h24 = (int(x) for x in v.split(","))
            filas.append((k, total, h24))
    return filas


# ---------- Figura: sedes por localidad ----------
loc = leer("resultado_localidad.txt")
loc = sorted(loc, key=lambda r: r[1])
ACENTOS = {"Fontibon": "Fontibón", "Usaquen": "Usaquén", "Engativa": "Engativá", "San Cristobal": "San Cristóbal",
           "Los Martires": "Los Mártires", "Ciudad Bolivar": "Ciudad Bolívar", "Candelaria": "La Candelaria"}
nombres = [ACENTOS.get(r[0].split(" - ", 1)[1].title(), r[0].split(" - ", 1)[1].title()) for r in loc]
tot = [r[1] for r in loc]
h24 = [r[2] for r in loc]

fig, ax = plt.subplots(figsize=(7.2, 5.6), dpi=200)
y = range(len(loc))
ax.barh([i + 0.2 for i in y], tot, height=0.4, color=GRIS, label="Sedes totales")
ax.barh([i - 0.2 for i in y], h24, height=0.4, color=AZUL, label="Sedes con atención 24 h")
for i, (t, h) in enumerate(zip(tot, h24)):
    ax.text(t + 0.2, i + 0.2, str(t), va="center", fontsize=7.5, color=TINTA)
    ax.text(h + 0.2, i - 0.2, str(h), va="center", fontsize=7.5, color=AZUL)
ax.set_yticks(list(y))
ax.set_yticklabels(nombres)
ax.set_xlabel("Número de sedes")
ax.set_title("Sedes de la red pública de salud por localidad (resultado del job MapReduce)", fontsize=9.5)
ax.grid(axis="x", color="#e1e1e1")
ax.set_axisbelow(True)
ax.legend(loc="lower right", frameon=False)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "fig_sedes_por_localidad.png"))
plt.close(fig)

# ---------- Figura: porcentaje 24h por subred ----------
sub = leer("resultado_subred.txt")
etiquetas = {
    "SUBRED INTEGRADA DE SERVICIOS DE SALUD CENTRO ORIENTE E.S.E": "Centro Oriente",
    "SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE E.S.E": "Norte",
    "SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR E.S.E.": "Sur",
    "Subred Integrada de Servicios de Salud Sur Occidente E.S.E": "Sur Occidente",
}
sub = sorted(sub, key=lambda r: r[2] / r[1])
nom = [etiquetas[r[0]] for r in sub]
pct = [100.0 * r[2] / r[1] for r in sub]

fig, ax = plt.subplots(figsize=(7.2, 3.2), dpi=200)
barras = ax.barh(nom, pct, color=AZUL, height=0.55)
for b, r, p in zip(barras, sub, pct):
    ax.text(p + 1, b.get_y() + b.get_height() / 2, f"{p:.1f} %  ({r[2]} de {r[1]} sedes)",
            va="center", fontsize=8.5, color=TINTA)
ax.set_xlim(0, 80)
ax.set_xlabel("Sedes con atención 24 horas (%)")
ax.set_title("Proporción de sedes con atención 24 h por subred", fontsize=9.5)
ax.grid(axis="x", color="#e1e1e1")
ax.set_axisbelow(True)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "fig_porcentaje_24h_subred.png"))
plt.close(fig)

# ---------- Figura: flujo MapReduce con contadores reales ----------
fig, ax = plt.subplots(figsize=(7.6, 2.6), dpi=200)
ax.set_xlim(-1, 107)
ax.set_ylim(0, 40)
ax.axis("off")
etapas = [
    ("Entrada HDFS", "104 líneas\n57 837 bytes", "#e9edf5"),
    ("Map", "103 pares\n(clave, \"1,e\")", "#d3dcee"),
    ("Combine", "103 → 19\nregistros", "#bccae6"),
    ("Shuffle/sort", "19 claves\nagrupadas", "#a5b7dd"),
    ("Reduce", "19 grupos\n19 salidas", "#8ea4d3"),
    ("Salida HDFS", "19 líneas\n367 bytes", "#e9edf5"),
]
w, g = 15.0, 2.6
x0 = 1.0
for i, (t, d, c) in enumerate(etapas):
    x = x0 + i * (w + g)
    ax.add_patch(FancyBboxPatch((x, 10), w, 20, boxstyle="round,pad=0.4,rounding_size=1.2",
                                fc=c, ec=AZUL, lw=0.9))
    ax.text(x + w / 2, 25.2, t, ha="center", va="center", fontsize=7.4, fontweight="bold", color=TINTA)
    ax.text(x + w / 2, 16.5, d, ha="center", va="center", fontsize=7.2, color=TINTA)
    if i < len(etapas) - 1:
        ax.add_patch(FancyArrowPatch((x + w + 0.6, 20), (x + w + g - 0.6, 20),
                                     arrowstyle="-|>", mutation_scale=9, color=AZUL, lw=1))
ax.text(52, 4, "Job ConteoSedes (modo localidad): contadores reportados por Hadoop 3.2.1",
        ha="center", fontsize=8, color="#555555")
fig.savefig(os.path.join(OUT, "fig_flujo_mapreduce.png"), bbox_inches="tight")
plt.close(fig)
print("ok", os.listdir(OUT))
