"""
StockReserv v3 — Gestion de Stock & Reservations
"""
import streamlit as st
import pandas as pd
import sqlite3
import os
import math
from datetime import datetime, date
from contextlib import contextmanager

# ================================================================
# CONFIGURATION
# ================================================================
COLUMN_MAPPING = {
    "vcd":              ["VCD", "vcd"],
    "ref_fournisseur":  ["Réf Fournisseur Principal", "Ref Fournisseur Principal"],
    "libelle":          ["Libelle Complet", "Libellé Complet", "libelle complet"],
    "stock_brut":       ["Qté Livr/Aff Ligne", "Qte Livr/Aff Ligne"],
    "marque":           ["Marque", "marque"],
    "affichage":        ["Affichage", "affichage"],
    "processeur":       ["Processeur", "processeur"],
    "memoire":          ["Mémoire", "Memoire", "mémoire"],
    "stockage":         ["Stockage", "stockage"],
    "pv_resah":         ["PV au Resah", "pv au resah", "PV au RESAH"],
    "tx_marge":         ["Tx de marge", "tx de marge", "Taux de marge"],
    "marge_unitaire":   ["Montant marge unitaire", "montant marge unitaire"],
}
REQUIRED_COLUMNS = ["vcd", "libelle", "stock_brut"]

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "stockreserv.db")

# ================================================================
# SAFE CONVERTERS
# ================================================================
def safe_int(val, default=0):
    if val is None:
        return default
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

def safe_float(val, default=0.0):
    if val is None:
        return default
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_str(val, default=""):
    if val is None:
        return default
    s = str(val).strip()
    if s.lower() in ("nan", "none", "nat", ""):
        return default
    return s

# ================================================================
# BASE DE DONNEES
# ================================================================
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
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
                vcd TEXT PRIMARY KEY,
                ref_fournisseur TEXT DEFAULT '',
                libelle TEXT DEFAULT '',
                stock_brut INTEGER DEFAULT 0,
                marque TEXT DEFAULT '',
                affichage TEXT DEFAULT '',
                processeur TEXT DEFAULT '',
                memoire TEXT DEFAULT '',
                stockage TEXT DEFAULT '',
                pv_resah REAL DEFAULT 0,
                tx_marge REAL DEFAULT 0,
                marge_unitaire REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                personne TEXT NOT NULL,
                vcd TEXT NOT NULL,
                quantite INTEGER NOT NULL,
                commentaire TEXT DEFAULT '',
                date_reservation DATE DEFAULT CURRENT_DATE,
                statut TEXT CHECK(statut IN ('actif','annule','consomme')) DEFAULT 'actif',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (vcd) REFERENCES produits(vcd)
            );
        """)

def qry(sql, params=(), fetch="all"):
    with get_db() as conn:
        cur = conn.execute(sql, params)
        if fetch == "all":
            return [dict(r) for r in cur.fetchall()]
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
        return False, f"Colonnes introuvables : {', '.join(noms)}\nColonnes detectees : {list(df.columns)}"

    # Construire les records en gerant les doublons VCD
    # Si un VCD apparait plusieurs fois, on garde la derniere ligne
    # et on ADDITIONNE les quantites
    raw_records = {}
    skipped = 0
    for _, row in df.iterrows():
        vcd = safe_str(row.get(mapping.get("vcd", ""), ""))
        if not vcd:
            skipped += 1
            continue

        stock = safe_int(row.get(mapping.get("stock_brut", ""), 0))

        if vcd in raw_records:
            # Doublon : on additionne le stock
            raw_records[vcd]["stock_brut"] += stock
        else:
            raw_records[vcd] = {
                "vcd": vcd,
                "ref_fournisseur": safe_str(row.get(mapping.get("ref_fournisseur", ""), "")),
                "libelle": safe_str(row.get(mapping.get("libelle", ""), "")),
                "stock_brut": stock,
                "marque": safe_str(row.get(mapping.get("marque", ""), "")),
                "affichage": safe_str(row.get(mapping.get("affichage", ""), "")),
                "processeur": safe_str(row.get(mapping.get("processeur", ""), "")),
                "memoire": safe_str(row.get(mapping.get("memoire", ""), "")),
                "stockage": safe_str(row.get(mapping.get("stockage", ""), "")),
                "pv_resah": safe_float(row.get(mapping.get("pv_resah", ""), 0)),
                "tx_marge": safe_float(row.get(mapping.get("tx_marge", ""), 0)),
                "marge_unitaire": safe_float(row.get(mapping.get("marge_unitaire", ""), 0)),
            }

    records = list(raw_records.values())
    if not records:
        return False, "Aucun produit trouve dans le fichier."

    doublons = len(df) - skipped - len(records)

    with get_db() as conn:
        if mode == "hebdo":
            updated, new = 0, 0
            for r in records:
                existing = conn.execute("SELECT vcd FROM produits WHERE vcd = ?", (r["vcd"],)).fetchone()
                if existing:
                    conn.execute(
                        "UPDATE produits SET stock_brut = ?, updated_at = CURRENT_TIMESTAMP WHERE vcd = ?",
                        (r["stock_brut"], r["vcd"]))
                    updated += 1
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO produits
                        (vcd, ref_fournisseur, libelle, stock_brut, marque, affichage,
                         processeur, memoire, stockage, pv_resah, tx_marge, marge_unitaire, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (r["vcd"], r["ref_fournisseur"], r["libelle"], r["stock_brut"],
                          r["marque"], r["affichage"], r["processeur"], r["memoire"],
                          r["stockage"], r["pv_resah"], r["tx_marge"], r["marge_unitaire"]))
                    new += 1
            msg = f"Mise a jour hebdo : {updated} stocks mis a jour"
            if new: msg += f", {new} nouveaux produits"
            if doublons > 0: msg += f" ({doublons} doublons VCD fusionnes)"
            if skipped: msg += f" ({skipped} lignes vides ignorees)"
            return True, msg
        else:
            conn.execute("DELETE FROM produits")
            for r in records:
                conn.execute("""
                    INSERT OR REPLACE INTO produits
                    (vcd, ref_fournisseur, libelle, stock_brut, marque, affichage,
                     processeur, memoire, stockage, pv_resah, tx_marge, marge_unitaire, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (r["vcd"], r["ref_fournisseur"], r["libelle"], r["stock_brut"],
                      r["marque"], r["affichage"], r["processeur"], r["memoire"],
                      r["stockage"], r["pv_resah"], r["tx_marge"], r["marge_unitaire"]))
            msg = f"Import complet : {len(records)} produits charges !"
            if doublons > 0: msg += f" ({doublons} doublons VCD fusionnes, stocks additionnes)"
            if skipped: msg += f" ({skipped} lignes vides ignorees)"
            return True, msg

# ================================================================
# FONCTIONS STOCK & RESERVATIONS
# ================================================================
def get_produits_with_stock():
    return qry("""
        SELECT p.*,
            p.stock_brut - COALESCE(SUM(CASE WHEN r.statut = 'actif' THEN r.quantite ELSE 0 END), 0) AS stock_disponible,
            COALESCE(SUM(CASE WHEN r.statut = 'actif' THEN r.quantite ELSE 0 END), 0) AS total_reserve
        FROM produits p LEFT JOIN reservations r ON p.vcd = r.vcd
        GROUP BY p.vcd ORDER BY p.libelle
    """)

def get_alertes():
    return qry("""
        SELECT p.vcd, p.libelle, p.stock_brut, COALESCE(SUM(r.quantite), 0) AS total_reserve
        FROM produits p JOIN reservations r ON p.vcd = r.vcd AND r.statut = 'actif'
        GROUP BY p.vcd HAVING p.stock_brut < total_reserve
    """)

def get_reservations(statut_filter=None):
    sql = "SELECT r.*, p.libelle FROM reservations r LEFT JOIN produits p ON r.vcd = p.vcd"
    params = []
    if statut_filter:
        sql += " WHERE r.statut = ?"
        params.append(statut_filter)
    sql += " ORDER BY r.created_at DESC"
    return qry(sql, tuple(params))

def creer_reservation(personne, vcd, quantite, commentaire, date_resa):
    produit = qry("SELECT * FROM produits WHERE vcd = ?", (vcd,), fetch="one")
    if not produit:
        return False, "VCD introuvable."
    reserves = qry(
        "SELECT COALESCE(SUM(quantite), 0) as total FROM reservations WHERE vcd = ? AND statut = 'actif'",
        (vcd,), fetch="one")
    stock_dispo = produit["stock_brut"] - (reserves["total"] if reserves else 0)
    if quantite > stock_dispo:
        return False, f"Stock insuffisant ! Dispo : {stock_dispo}, demande : {quantite}"
    qry("INSERT INTO reservations (personne, vcd, quantite, commentaire, date_reservation) VALUES (?, ?, ?, ?, ?)",
        (personne, vcd, quantite, commentaire, date_resa), fetch="one")
    return True, f"Reservation creee : {quantite}x {vcd} pour {personne}"

def update_statut_reservation(resa_id, new_statut):
    qry("UPDATE reservations SET statut = ? WHERE id = ?", (new_statut, resa_id), fetch="one")

def update_produit_field(vcd, field, value):
    allowed = {"ref_fournisseur", "libelle", "stock_brut", "marque", "affichage",
               "processeur", "memoire", "stockage", "pv_resah", "tx_marge", "marge_unitaire"}
    if field not in allowed:
        return
    qry(f"UPDATE produits SET {field} = ?, updated_at = CURRENT_TIMESTAMP WHERE vcd = ?", (value, vcd), fetch="one")

# ================================================================
# INTERFACE
# ================================================================
st.set_page_config(page_title="StockReserv", page_icon="📦", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0F1923; }
    .header-box {
        background: linear-gradient(135deg, #0D1B2A, #1A2D42);
        border-bottom: 3px solid #E88D3F;
        padding: 18px 24px; border-radius: 10px; margin-bottom: 20px;
    }
    .header-box h1 { color: #E88D3F !important; margin: 0 !important; font-size: 1.7em !important; }
    .header-box p { color: #A0AEC1; margin: 4px 0 0; }
    .alert-box {
        background: #3D1F1F; border-left: 4px solid #E74C3C;
        padding: 12px 16px; border-radius: 0 8px 8px 0; margin: 8px 0; color: #F5B7B1;
    }
    .stat-card {
        background: #1A2D42; border-radius: 8px; padding: 16px;
        text-align: center; border: 1px solid #2A3F55;
    }
    .stat-card h2 { color: #1B998B !important; margin: 0 !important; }
    .stat-card p { color: #A0AEC1; margin: 4px 0 0; }
    .resa-card { border-radius: 8px; padding: 14px; margin: 6px 0; border: 1px solid #2A3F55; }
</style>
""", unsafe_allow_html=True)

init_db()

st.markdown("""
<div class="header-box">
    <h1>📦 StockReserv</h1>
    <p>Gestion de stock & reservations equipe — partagez le lien</p>
</div>
""", unsafe_allow_html=True)

# -- Sidebar --
with st.sidebar:
    st.markdown("### 📂 Import Excel")
    import_mode = st.radio(
        "Type d'import :",
        ["📥 Premier import (tout charger)", "🔄 Mise à jour hebdo (stock uniquement)"])
    mode = "premier" if "Premier" in import_mode else "hebdo"

    uploaded = st.file_uploader("Fichier stock (.xlsx)", type=["xlsx", "xls"])
    if uploaded:
        ok, msg = import_excel(uploaded, mode=mode)
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    st.divider()
    st.markdown("### ℹ️ Mode d'emploi")
    st.caption(
        "1er usage → Premier import\n"
        "Chaque semaine → Mise à jour hebdo\n"
        "   (seul Qté Livr/Aff Ligne change)\n"
        "Onglet Édition → modifier en live\n"
        "Onglet Réserver → créer une résa\n"
        "Onglet Réservations → gérer")

# -- Alertes --
alertes = get_alertes()
if alertes:
    for a in alertes:
        st.markdown(
            f'<div class="alert-box">⚠️ <b>{a["vcd"]}</b> — {a["libelle"]} : '
            f'stock brut ({a["stock_brut"]}) &lt; reservations actives ({a["total_reserve"]})</div>',
            unsafe_allow_html=True)

# -- Stats --
produits = get_produits_with_stock()
total_refs = len(produits)
total_stock = sum(p["stock_brut"] for p in produits)
total_reserve = sum(p["total_reserve"] for p in produits)
total_dispo = sum(max(p["stock_disponible"], 0) for p in produits)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="stat-card"><h2>{total_refs}</h2><p>References</p></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="stat-card"><h2>{total_stock}</h2><p>Stock brut</p></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="stat-card"><h2>{total_reserve}</h2><p>Reserve</p></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="stat-card"><h2>{total_dispo}</h2><p>Disponible</p></div>', unsafe_allow_html=True)

st.markdown("")

tab_produits, tab_edit, tab_reserver, tab_reservations = st.tabs([
    "📦 Produits & Stock", "✏️ Édition Live", "➕ Réserver", "📋 Réservations"
])

# --- TAB PRODUITS ---
with tab_produits:
    if not produits:
        st.info("📂 Aucun produit. Importe un fichier Excel via le sidebar.")
    else:
        col_search, col_marque, col_filter = st.columns([2, 1, 1])
        with col_search:
            search = st.text_input("🔍 Recherche (VCD, libellé, réf fournisseur)", key="sp")
        with col_marque:
            marques = sorted(set(p["marque"] for p in produits if p["marque"]))
            marque_filter = st.selectbox("Marque", ["Toutes"] + marques, key="mf")
        with col_filter:
            only_dispo = st.checkbox("Stock dispo > 0", value=False, key="od")

        df = pd.DataFrame(produits)
        if search:
            s = search.lower()
            mask = (df["vcd"].astype(str).str.lower().str.contains(s, na=False)
                    | df["libelle"].astype(str).str.lower().str.contains(s, na=False)
                    | df["ref_fournisseur"].astype(str).str.lower().str.contains(s, na=False))
            df = df[mask]
        if marque_filter != "Toutes":
            df = df[df["marque"] == marque_filter]
        if only_dispo:
            df = df[df["stock_disponible"] > 0]

        display_df = df[["vcd", "ref_fournisseur", "libelle", "marque", "affichage", "processeur",
                         "memoire", "stockage", "pv_resah", "tx_marge", "marge_unitaire",
                         "stock_brut", "total_reserve", "stock_disponible"]].copy()
        display_df.columns = ["VCD", "Réf Fournisseur", "Libellé", "Marque", "Affichage", "Processeur",
                              "Mémoire", "Stockage", "PV Resah €", "Tx Marge %", "Marge Unit. €",
                              "Stock Brut", "Réservé", "Disponible"]

        def color_stock(val):
            if isinstance(val, (int, float)):
                if val <= 0: return "background-color: #5C1A1A; color: #F5B7B1"
                elif val < 10: return "background-color: #5C4B1A; color: #F9E79F"
            return ""

        styled = display_df.style.map(color_stock, subset=["Disponible"])
        st.dataframe(styled, use_container_width=True, hide_index=True, height=500)
        st.caption(f"{len(display_df)} produit(s) sur {total_refs}")

# --- TAB EDITION LIVE ---
with tab_edit:
    if not produits:
        st.info("📂 Importe d'abord un fichier Excel.")
    else:
        st.markdown("#### ✏️ Clique sur une cellule pour la modifier, puis sauvegarde")

        edit_df = pd.DataFrame(produits)
        edit_display = edit_df[["vcd", "ref_fournisseur", "libelle", "marque", "affichage", "processeur",
                                "memoire", "stockage", "pv_resah", "tx_marge", "marge_unitaire", "stock_brut"]].copy()
        edit_display.columns = ["VCD", "Réf Fournisseur", "Libellé", "Marque", "Affichage", "Processeur",
                                "Mémoire", "Stockage", "PV Resah €", "Tx Marge %", "Marge Unit. €", "Stock Brut"]

        edited = st.data_editor(
            edit_display, use_container_width=True, hide_index=True, height=500,
            num_rows="fixed", disabled=["VCD"],
            column_config={
                "VCD": st.column_config.TextColumn("VCD", width="small"),
                "Stock Brut": st.column_config.NumberColumn("Stock Brut", min_value=0, step=1),
                "PV Resah €": st.column_config.NumberColumn("PV Resah €", min_value=0, format="%.2f"),
                "Tx Marge %": st.column_config.NumberColumn("Tx Marge %", format="%.2f"),
                "Marge Unit. €": st.column_config.NumberColumn("Marge Unit. €", format="%.2f"),
            }, key="product_editor")

        if st.button("💾 Sauvegarder", type="primary", use_container_width=True):
            col_map = {"Réf Fournisseur": "ref_fournisseur", "Libellé": "libelle", "Marque": "marque",
                       "Affichage": "affichage", "Processeur": "processeur", "Mémoire": "memoire",
                       "Stockage": "stockage", "PV Resah €": "pv_resah", "Tx Marge %": "tx_marge",
                       "Marge Unit. €": "marge_unitaire", "Stock Brut": "stock_brut"}
            changes = 0
            for idx, row in edited.iterrows():
                vcd = row["VCD"]
                orig = edit_display.iloc[idx]
                for display_col, db_col in col_map.items():
                    if str(row[display_col]) != str(orig[display_col]):
                        update_produit_field(vcd, db_col, row[display_col])
                        changes += 1
            if changes:
                st.success(f"{changes} modification(s) sauvegardee(s) !")
                st.rerun()
            else:
                st.info("Aucune modification.")

# --- TAB RESERVER ---
with tab_reserver:
    if not produits:
        st.info("📂 Importe d'abord un fichier Excel.")
    else:
        st.markdown("#### ➕ Nouvelle réservation")
        col1, col2 = st.columns(2)
        with col1:
            personne = st.text_input("👤 Nom", placeholder="Romain, Lisa...", key="rn")
            vcds = [p["vcd"] for p in produits]
            vcd_select = st.selectbox("📦 Produit (VCD)", vcds,
                format_func=lambda v: f"{v} — {next((p['libelle'][:60] for p in produits if p['vcd'] == v), '')}",
                key="rv")
        with col2:
            prod_info = next((p for p in produits if p["vcd"] == vcd_select), None)
            if prod_info:
                st.markdown(f"""
                **{prod_info['libelle']}**  
                🏷️ {prod_info['marque']} | Réf : {prod_info['ref_fournisseur']}  
                💰 PV : {prod_info['pv_resah']} € | Stock : **{prod_info['stock_brut']}** | Réservé : **{prod_info['total_reserve']}** | Dispo : **{max(prod_info['stock_disponible'], 0)}**
                """)
                max_qty = max(prod_info["stock_disponible"], 0)
            else:
                max_qty = 0
            quantite = st.number_input("Quantité", min_value=1, max_value=max(max_qty, 1), value=1, key="rq")
            date_resa = st.date_input("📅 Date", value=date.today(), key="rd")
        commentaire = st.text_area("💬 Commentaire", placeholder="Devis client X...", key="rc")

        if st.button("✅ Créer la réservation", type="primary", use_container_width=True):
            if not personne.strip():
                st.error("Indique le nom.")
            else:
                ok, msg = creer_reservation(personne.strip(), vcd_select, quantite, commentaire.strip(), date_resa.isoformat())
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

# --- TAB RESERVATIONS ---
with tab_reservations:
    st.markdown("#### 📋 Réservations")
    filtre = st.selectbox("Statut", ["Tous", "actif", "annule", "consomme"], key="fs")
    statut_param = None if filtre == "Tous" else filtre

    resas = get_reservations(statut_param)
    if not resas:
        st.info("Aucune réservation.")
    else:
        for r in resas:
            emoji = {"actif": "🟢", "annule": "🔴", "consomme": "✅"}.get(r["statut"], "⚪")
            cbg = {"actif": "#1A3D2A", "annule": "#3D1F1F", "consomme": "#1A2D42"}.get(r["statut"], "#1A2D42")
            label = {"actif": "Actif", "annule": "Annulé", "consomme": "Consommé"}.get(r["statut"], r["statut"])

            st.markdown(f"""
            <div class="resa-card" style="background:{cbg};">
                <strong>{emoji} #{r['id']}</strong> — <b>{r['personne']}</b> →
                {r['quantite']}x <code>{r['vcd']}</code> ({r.get('libelle', '')})
                <br/><small>📅 {r['date_reservation']} | 💬 {r.get('commentaire', '') or '—'} | {label}</small>
            </div>""", unsafe_allow_html=True)

            if r["statut"] == "actif":
                b1, b2, b3 = st.columns([1, 1, 4])
                with b1:
                    if st.button("✅ Consommé", key=f"c_{r['id']}"):
                        update_statut_reservation(r["id"], "consomme")
                        st.rerun()
                with b2:
                    if st.button("❌ Annuler", key=f"a_{r['id']}"):
                        update_statut_reservation(r["id"], "annule")
                        st.rerun()
        st.caption(f"{len(resas)} réservation(s)")
