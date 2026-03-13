"""
StockReserv v4 — Cle primaire = Article (pas VCD)
"""
import streamlit as st
import pandas as pd
import sqlite3
import os
import math
from datetime import datetime, date
from contextlib import contextmanager

# ================================================================
# CONFIGURATION - colonnes exactes de ton Excel
# ================================================================
COLUMN_MAPPING = {
    "article":          ["Article", "article"],
    "groupe":           ["Groupe", "groupe"],
    "code_ic1":         ["Code IC1 Ventes", "code ic1 ventes"],
    "vcd":              ["VCD", "vcd"],
    "date_saisie":      ["Date Saisie Cde", "date saisie cde"],
    "num_projet":       ["NUMERO PROJET", "numero projet"],
    "nom_projet":       ["Nom du Projet", "nom du projet"],
    "ref_fournisseur":  ["Réf Fournisseur Principal", "Ref Fournisseur Principal"],
    "libelle":          ["Libelle Complet", "Libellé Complet"],
    "marque":           ["Marque", "marque"],
    "affichage":        ["Affichage", "affichage"],
    "processeur":       ["Processeur", "processeur"],
    "memoire":          ["Mémoire", "Memoire"],
    "stockage":         ["Stockage", "stockage"],
    "qte_commandee":    ["Qté Commandée Ligne", "Qte Commandee Ligne"],
    "stock_brut":       ["Qté Livr/Aff Ligne", "Qte Livr/Aff Ligne"],
    "prix_ha_scc":      ["Prix Unitaire HA SCC", "prix unitaire ha scc"],
    "pv_resah":         ["PV au Resah", "pv au resah"],
    "pv_client":        ["PV Client(marge Resah incluse)", "pv client"],
    "tx_marge":         ["Tx de marge", "tx de marge"],
    "marge_unitaire":   ["Montant marge unitaire", "montant marge unitaire"],
}
REQUIRED_COLUMNS = ["article", "libelle", "stock_brut"]

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "stockreserv.db")

# ================================================================
# SAFE CONVERTERS
# ================================================================
def safe_int(val, default=0):
    if val is None: return default
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)): return default
    try: return int(float(val))
    except (ValueError, TypeError): return default

def safe_float(val, default=0.0):
    if val is None: return default
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)): return default
    try: return float(val)
    except (ValueError, TypeError): return default

def safe_str(val, default=""):
    if val is None: return default
    s = str(val).strip()
    if s.lower() in ("nan", "none", "nat", ""): return default
    return s

# ================================================================
# BASE DE DONNEES
# ================================================================
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS produits (
                article TEXT PRIMARY KEY,
                groupe TEXT DEFAULT '',
                code_ic1 TEXT DEFAULT '',
                vcd TEXT DEFAULT '',
                num_projet TEXT DEFAULT '',
                nom_projet TEXT DEFAULT '',
                ref_fournisseur TEXT DEFAULT '',
                libelle TEXT DEFAULT '',
                marque TEXT DEFAULT '',
                affichage TEXT DEFAULT '',
                processeur TEXT DEFAULT '',
                memoire TEXT DEFAULT '',
                stockage TEXT DEFAULT '',
                qte_commandee INTEGER DEFAULT 0,
                stock_brut INTEGER DEFAULT 0,
                prix_ha_scc REAL DEFAULT 0,
                pv_resah REAL DEFAULT 0,
                pv_client REAL DEFAULT 0,
                tx_marge REAL DEFAULT 0,
                marge_unitaire REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                personne TEXT NOT NULL,
                article TEXT NOT NULL,
                quantite INTEGER NOT NULL,
                commentaire TEXT DEFAULT '',
                date_reservation DATE DEFAULT CURRENT_DATE,
                statut TEXT CHECK(statut IN ('actif','annule','consomme')) DEFAULT 'actif',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (article) REFERENCES produits(article)
            );
        """)

def qry(sql, params=(), fetch="all"):
    with get_db() as conn:
        cur = conn.execute(sql, params)
        if fetch == "all": return [dict(r) for r in cur.fetchall()]
        elif fetch == "one":
            r = cur.fetchone()
            return dict(r) if r else None
        return cur.lastrowid

# ================================================================
# IMPORT EXCEL
# ================================================================
def find_column(df_columns, possible_names):
    for name in possible_names:
        for col in df_columns:
            if col.strip().lower() == name.strip().lower():
                return col
    return None

def import_excel(uploaded_file, mode="premier"):
    df = pd.read_excel(uploaded_file)
    mapping = {}
    for key, possible in COLUMN_MAPPING.items():
        found = find_column(df.columns, possible)
        if found:
            mapping[key] = found

    missing = [k for k in REQUIRED_COLUMNS if k not in mapping]
    if missing:
        noms = [COLUMN_MAPPING[k][0] for k in missing]
        return False, f"Colonnes introuvables : {', '.join(noms)}\nDetectees : {list(df.columns)}"

    records = []
    skipped = 0
    for _, row in df.iterrows():
        article = safe_str(row.get(mapping.get("article", ""), ""))
        libelle = safe_str(row.get(mapping.get("libelle", ""), ""))
        if not article or not libelle:
            skipped += 1
            continue
        records.append({
            "article": article,
            "groupe": safe_str(row.get(mapping.get("groupe", ""), "")),
            "code_ic1": safe_str(row.get(mapping.get("code_ic1", ""), "")),
            "vcd": safe_str(row.get(mapping.get("vcd", ""), "")),
            "num_projet": safe_str(row.get(mapping.get("num_projet", ""), "")),
            "nom_projet": safe_str(row.get(mapping.get("nom_projet", ""), "")),
            "ref_fournisseur": safe_str(row.get(mapping.get("ref_fournisseur", ""), "")),
            "libelle": libelle,
            "marque": safe_str(row.get(mapping.get("marque", ""), "")),
            "affichage": safe_str(row.get(mapping.get("affichage", ""), "")),
            "processeur": safe_str(row.get(mapping.get("processeur", ""), "")),
            "memoire": safe_str(row.get(mapping.get("memoire", ""), "")),
            "stockage": safe_str(row.get(mapping.get("stockage", ""), "")),
            "qte_commandee": safe_int(row.get(mapping.get("qte_commandee", ""), 0)),
            "stock_brut": safe_int(row.get(mapping.get("stock_brut", ""), 0)),
            "prix_ha_scc": safe_float(row.get(mapping.get("prix_ha_scc", ""), 0)),
            "pv_resah": safe_float(row.get(mapping.get("pv_resah", ""), 0)),
            "pv_client": safe_float(row.get(mapping.get("pv_client", ""), 0)),
            "tx_marge": safe_float(row.get(mapping.get("tx_marge", ""), 0)),
            "marge_unitaire": safe_float(row.get(mapping.get("marge_unitaire", ""), 0)),
        })

    if not records:
        return False, "Aucun produit trouve."

    with get_db() as conn:
        if mode == "hebdo":
            updated, new = 0, 0
            for r in records:
                existing = conn.execute("SELECT article FROM produits WHERE article=?", (r["article"],)).fetchone()
                if existing:
                    conn.execute("UPDATE produits SET stock_brut=?, updated_at=CURRENT_TIMESTAMP WHERE article=?",
                                 (r["stock_brut"], r["article"]))
                    updated += 1
                else:
                    _insert_produit(conn, r)
                    new += 1
            msg = f"Hebdo : {updated} stocks MAJ"
            if new: msg += f", {new} nouveaux"
            if skipped: msg += f" ({skipped} lignes vides ignorees)"
            return True, msg
        else:
            conn.execute("DELETE FROM produits")
            for r in records:
                _insert_produit(conn, r)
            msg = f"Import complet : {len(records)} produits !"
            if skipped: msg += f" ({skipped} lignes vides ignorees)"
            return True, msg

def _insert_produit(conn, r):
    conn.execute("""
        INSERT OR REPLACE INTO produits
        (article, groupe, code_ic1, vcd, num_projet, nom_projet, ref_fournisseur, libelle,
         marque, affichage, processeur, memoire, stockage, qte_commandee, stock_brut,
         prix_ha_scc, pv_resah, pv_client, tx_marge, marge_unitaire, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
    """, (r["article"], r["groupe"], r["code_ic1"], r["vcd"], r["num_projet"], r["nom_projet"],
          r["ref_fournisseur"], r["libelle"], r["marque"], r["affichage"], r["processeur"],
          r["memoire"], r["stockage"], r["qte_commandee"], r["stock_brut"],
          r["prix_ha_scc"], r["pv_resah"], r["pv_client"], r["tx_marge"], r["marge_unitaire"]))

# ================================================================
# FONCTIONS STOCK & RESERVATIONS
# ================================================================
def get_produits_with_stock():
    return qry("""
        SELECT p.*,
            p.stock_brut - COALESCE(SUM(CASE WHEN r.statut='actif' THEN r.quantite ELSE 0 END),0) AS stock_disponible,
            COALESCE(SUM(CASE WHEN r.statut='actif' THEN r.quantite ELSE 0 END),0) AS total_reserve
        FROM produits p LEFT JOIN reservations r ON p.article=r.article
        GROUP BY p.article ORDER BY p.marque, p.libelle
    """)

def get_alertes():
    return qry("""
        SELECT p.article, p.libelle, p.stock_brut, COALESCE(SUM(r.quantite),0) AS total_reserve
        FROM produits p JOIN reservations r ON p.article=r.article AND r.statut='actif'
        GROUP BY p.article HAVING p.stock_brut < total_reserve
    """)

def get_reservations(statut_filter=None):
    sql = "SELECT r.*, p.libelle, p.marque FROM reservations r LEFT JOIN produits p ON r.article=p.article"
    params = []
    if statut_filter:
        sql += " WHERE r.statut=?"
        params.append(statut_filter)
    sql += " ORDER BY r.created_at DESC"
    return qry(sql, tuple(params))

def creer_reservation(personne, article, quantite, commentaire, date_resa):
    produit = qry("SELECT * FROM produits WHERE article=?", (article,), fetch="one")
    if not produit: return False, "Article introuvable."
    reserves = qry("SELECT COALESCE(SUM(quantite),0) as total FROM reservations WHERE article=? AND statut='actif'",
                    (article,), fetch="one")
    stock_dispo = produit["stock_brut"] - (reserves["total"] if reserves else 0)
    if quantite > stock_dispo:
        return False, f"Stock insuffisant ! Dispo : {stock_dispo}, demande : {quantite}"
    qry("INSERT INTO reservations (personne, article, quantite, commentaire, date_reservation) VALUES (?,?,?,?,?)",
        (personne, article, quantite, commentaire, date_resa), fetch="one")
    return True, f"Reservation : {quantite}x {article} pour {personne}"

def update_statut_reservation(resa_id, new_statut):
    qry("UPDATE reservations SET statut=? WHERE id=?", (new_statut, resa_id), fetch="one")

def update_produit_field(article, field, value):
    allowed = {"ref_fournisseur","libelle","stock_brut","marque","affichage","processeur",
               "memoire","stockage","pv_resah","tx_marge","marge_unitaire","qte_commandee",
               "prix_ha_scc","pv_client"}
    if field not in allowed: return
    qry(f"UPDATE produits SET {field}=?, updated_at=CURRENT_TIMESTAMP WHERE article=?", (value, article), fetch="one")

# ================================================================
# INTERFACE
# ================================================================
st.set_page_config(page_title="StockReserv", page_icon="📦", layout="wide")
st.markdown("""
<style>
    .stApp { background-color: #0F1923; }
    .header-box { background:linear-gradient(135deg,#0D1B2A,#1A2D42); border-bottom:3px solid #E88D3F; padding:18px 24px; border-radius:10px; margin-bottom:20px; }
    .header-box h1 { color:#E88D3F !important; margin:0 !important; font-size:1.7em !important; }
    .header-box p { color:#A0AEC1; margin:4px 0 0; }
    .alert-box { background:#3D1F1F; border-left:4px solid #E74C3C; padding:12px 16px; border-radius:0 8px 8px 0; margin:8px 0; color:#F5B7B1; }
    .stat-card { background:#1A2D42; border-radius:8px; padding:16px; text-align:center; border:1px solid #2A3F55; }
    .stat-card h2 { color:#1B998B !important; margin:0 !important; }
    .stat-card p { color:#A0AEC1; margin:4px 0 0; }
    .resa-card { border-radius:8px; padding:14px; margin:6px 0; border:1px solid #2A3F55; }
</style>
""", unsafe_allow_html=True)

init_db()
st.markdown('<div class="header-box"><h1>📦 StockReserv</h1><p>Gestion de stock & reservations equipe</p></div>', unsafe_allow_html=True)

# -- Sidebar --
with st.sidebar:
    st.markdown("### 📂 Import Excel")
    import_mode = st.radio("Type :", ["📥 Premier import (tout)", "🔄 Hebdo (stock uniquement)"])
    mode = "premier" if "Premier" in import_mode else "hebdo"
    uploaded = st.file_uploader("Fichier .xlsx", type=["xlsx","xls"])
    if uploaded:
        ok, msg = import_excel(uploaded, mode=mode)
        if ok: st.success(msg)
        else: st.error(msg)
    st.divider()
    st.markdown("### ℹ️ Aide")
    st.caption("1er usage → Premier import\nChaque semaine → Hebdo\n(seul Qté Livr/Aff Ligne change)")

# -- Alertes --
alertes = get_alertes()
if alertes:
    for a in alertes:
        st.markdown(f'<div class="alert-box">⚠️ <b>{a["article"]}</b> — {a["libelle"]} : stock ({a["stock_brut"]}) &lt; reserves ({a["total_reserve"]})</div>', unsafe_allow_html=True)

# -- Stats --
produits = get_produits_with_stock()
total_refs = len(produits)
total_stock = sum(p["stock_brut"] for p in produits)
total_reserve = sum(p["total_reserve"] for p in produits)
total_dispo = sum(max(p["stock_disponible"],0) for p in produits)

c1,c2,c3,c4 = st.columns(4)
with c1: st.markdown(f'<div class="stat-card"><h2>{total_refs}</h2><p>Articles</p></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="stat-card"><h2>{total_stock}</h2><p>Stock brut</p></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="stat-card"><h2>{total_reserve}</h2><p>Réservé</p></div>', unsafe_allow_html=True)
with c4: st.markdown(f'<div class="stat-card"><h2>{total_dispo}</h2><p>Disponible</p></div>', unsafe_allow_html=True)

st.markdown("")
tab_produits, tab_edit, tab_reserver, tab_reservations = st.tabs(["📦 Produits","✏️ Édition","➕ Réserver","📋 Réservations"])

# --- PRODUITS ---
with tab_produits:
    if not produits:
        st.info("📂 Importe un Excel via le sidebar.")
    else:
        cs,cm,cf = st.columns([2,1,1])
        with cs: search = st.text_input("🔍 Recherche", key="sp")
        with cm:
            marques = sorted(set(p["marque"] for p in produits if p["marque"]))
            marque_f = st.selectbox("Marque", ["Toutes"]+marques, key="mf")
        with cf: only_dispo = st.checkbox("Dispo > 0", value=False, key="od")

        df = pd.DataFrame(produits)
        if search:
            s = search.lower()
            mask = (df["article"].astype(str).str.lower().str.contains(s,na=False)
                    | df["libelle"].astype(str).str.lower().str.contains(s,na=False)
                    | df["ref_fournisseur"].astype(str).str.lower().str.contains(s,na=False)
                    | df["vcd"].astype(str).str.lower().str.contains(s,na=False))
            df = df[mask]
        if marque_f != "Toutes": df = df[df["marque"]==marque_f]
        if only_dispo: df = df[df["stock_disponible"]>0]

        show = df[["article","vcd","ref_fournisseur","libelle","marque","processeur","memoire",
                    "stockage","affichage","qte_commandee","stock_brut","total_reserve",
                    "stock_disponible","pv_resah","tx_marge","marge_unitaire"]].copy()
        show.columns = ["Article","VCD","Réf Fourn.","Libellé","Marque","Processeur","Mémoire",
                        "Stockage","Affichage","Qté Cdée","Stock Brut","Réservé",
                        "Disponible","PV Resah €","Tx Marge %","Marge Unit. €"]

        def cs_fn(val):
            if isinstance(val,(int,float)):
                if val<=0: return "background-color:#5C1A1A;color:#F5B7B1"
                elif val<10: return "background-color:#5C4B1A;color:#F9E79F"
            return ""

        st.dataframe(show.style.map(cs_fn, subset=["Disponible"]), use_container_width=True, hide_index=True, height=500)
        st.caption(f"{len(show)} article(s) sur {total_refs}")

# --- EDITION ---
with tab_edit:
    if not produits:
        st.info("📂 Importe d'abord un Excel.")
    else:
        st.markdown("#### ✏️ Modifie les cellules puis sauvegarde")
        edf = pd.DataFrame(produits)
        ed = edf[["article","vcd","ref_fournisseur","libelle","marque","processeur","memoire",
                   "stockage","pv_resah","tx_marge","marge_unitaire","stock_brut"]].copy()
        ed.columns = ["Article","VCD","Réf Fourn.","Libellé","Marque","Processeur","Mémoire",
                      "Stockage","PV Resah €","Tx Marge %","Marge Unit. €","Stock Brut"]

        edited = st.data_editor(ed, use_container_width=True, hide_index=True, height=500,
            num_rows="fixed", disabled=["Article","VCD"],
            column_config={
                "Stock Brut": st.column_config.NumberColumn(min_value=0, step=1),
                "PV Resah €": st.column_config.NumberColumn(min_value=0, format="%.2f"),
            }, key="pe")

        if st.button("💾 Sauvegarder", type="primary", use_container_width=True):
            cmap = {"Réf Fourn.":"ref_fournisseur","Libellé":"libelle","Marque":"marque",
                    "Processeur":"processeur","Mémoire":"memoire","Stockage":"stockage",
                    "PV Resah €":"pv_resah","Tx Marge %":"tx_marge","Marge Unit. €":"marge_unitaire",
                    "Stock Brut":"stock_brut"}
            ch = 0
            for idx, row in edited.iterrows():
                art = row["Article"]
                orig = ed.iloc[idx]
                for dc, dbc in cmap.items():
                    if str(row[dc]) != str(orig[dc]):
                        update_produit_field(art, dbc, row[dc])
                        ch += 1
            if ch:
                st.success(f"{ch} modification(s) !")
                st.rerun()
            else: st.info("Aucune modification.")

# --- RESERVER ---
with tab_reserver:
    if not produits:
        st.info("📂 Importe d'abord un Excel.")
    else:
        st.markdown("#### ➕ Nouvelle réservation")
        c1,c2 = st.columns(2)
        with c1:
            personne = st.text_input("👤 Nom", placeholder="Romain, Lisa...", key="rn")
            arts = [p["article"] for p in produits]
            art_sel = st.selectbox("📦 Article", arts,
                format_func=lambda a: f"{a} — {next((p['libelle'][:50] for p in produits if p['article']==a),'')}", key="rv")
        with c2:
            pi = next((p for p in produits if p["article"]==art_sel), None)
            if pi:
                st.markdown(f"""**{pi['libelle']}**  
                🏷️ {pi['marque']} | VCD: {pi['vcd']} | Réf: {pi['ref_fournisseur']}  
                💰 PV: {pi['pv_resah']}€ | Stock: **{pi['stock_brut']}** | Réservé: **{pi['total_reserve']}** | Dispo: **{max(pi['stock_disponible'],0)}**""")
                mq = max(pi["stock_disponible"],0)
            else: mq = 0
            quantite = st.number_input("Quantité", min_value=1, max_value=max(mq,1), value=1, key="rq")
            date_r = st.date_input("📅 Date", value=date.today(), key="rd")
        comm = st.text_area("💬 Commentaire", placeholder="Devis client X...", key="rc")
        if st.button("✅ Réserver", type="primary", use_container_width=True):
            if not personne.strip(): st.error("Indique le nom.")
            else:
                ok, msg = creer_reservation(personne.strip(), art_sel, quantite, comm.strip(), date_r.isoformat())
                if ok:
                    st.success(msg)
                    st.rerun()
                else: st.error(msg)

# --- RESERVATIONS ---
with tab_reservations:
    st.markdown("#### 📋 Réservations")
    flt = st.selectbox("Statut", ["Tous","actif","annule","consomme"], key="fs")
    sp = None if flt=="Tous" else flt
    resas = get_reservations(sp)
    if not resas: st.info("Aucune réservation.")
    else:
        for r in resas:
            em = {"actif":"🟢","annule":"🔴","consomme":"✅"}.get(r["statut"],"⚪")
            bg = {"actif":"#1A3D2A","annule":"#3D1F1F","consomme":"#1A2D42"}.get(r["statut"],"#1A2D42")
            lb = {"actif":"Actif","annule":"Annulé","consomme":"Consommé"}.get(r["statut"],r["statut"])
            st.markdown(f'<div class="resa-card" style="background:{bg};"><strong>{em} #{r["id"]}</strong> — <b>{r["personne"]}</b> → {r["quantite"]}x <code>{r["article"]}</code> ({r.get("libelle","")}) <br/><small>📅 {r["date_reservation"]} | 💬 {r.get("commentaire","") or "—"} | {lb}</small></div>', unsafe_allow_html=True)
            if r["statut"]=="actif":
                b1,b2,b3 = st.columns([1,1,4])
                with b1:
                    if st.button("✅ Consommé", key=f"c_{r['id']}"):
                        update_statut_reservation(r["id"],"consomme")
                        st.rerun()
                with b2:
                    if st.button("❌ Annuler", key=f"a_{r['id']}"):
                        update_statut_reservation(r["id"],"annule")
                        st.rerun()
        st.caption(f"{len(resas)} réservation(s)")
