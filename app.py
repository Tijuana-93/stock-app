"""
StockReserv v5 — Cle = Article, auto-reset ancien schema
"""
import streamlit as st
import pandas as pd
import sqlite3
import os
import math
from datetime import date
from contextlib import contextmanager

COLUMN_MAPPING = {
    "article":          ["Article"],
    "groupe":           ["Groupe"],
    "code_ic1":         ["Code IC1 Ventes"],
    "vcd":              ["VCD"],
    "num_projet":       ["NUMERO PROJET"],
    "nom_projet":       ["Nom du Projet"],
    "ref_fournisseur":  ["Réf Fournisseur Principal", "Ref Fournisseur Principal"],
    "libelle":          ["Libelle Complet", "Libellé Complet"],
    "marque":           ["Marque"],
    "affichage":        ["Affichage"],
    "processeur":       ["Processeur"],
    "memoire":          ["Mémoire", "Memoire"],
    "stockage":         ["Stockage"],
    "qte_commandee":    ["Qté Commandée Ligne", "Qte Commandee Ligne"],
    "stock_brut":       ["Qté Livr/Aff Ligne", "Qte Livr/Aff Ligne"],
    "prix_ha_scc":      ["Prix Unitaire HA SCC"],
    "pv_resah":         ["PV au Resah"],
    "pv_client":        ["PV Client(marge Resah incluse)"],
    "tx_marge":         ["Tx de marge"],
    "marge_unitaire":   ["Montant marge unitaire"],
}
REQUIRED_COLUMNS = ["article", "libelle", "stock_brut"]

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "stockreserv.db")

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
    # Detecter ancien schema (vcd comme cle) et supprimer si besoin
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.execute("PRAGMA table_info(produits)")
            cols = [row[1] for row in cur.fetchall()]
            conn.close()
            if cols and "article" not in cols:
                os.remove(DB_PATH)
        except Exception:
            try: os.remove(DB_PATH)
            except: pass

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
        if found: mapping[key] = found

    missing = [k for k in REQUIRED_COLUMNS if k not in mapping]
    if missing:
        return False, f"Colonnes introuvables : {', '.join(missing)}\nDetectees : {list(df.columns)}"

    records = []
    skipped = 0
    for _, row in df.iterrows():
        article = safe_str(row.get(mapping.get("article", ""), ""))
        libelle = safe_str(row.get(mapping.get("libelle", ""), ""))
        if not article or not libelle:
            skipped += 1
            continue
        r = {}
        for key in COLUMN_MAPPING:
            col = mapping.get(key, "")
            val = row.get(col, "") if col else ""
            if key in ("stock_brut", "qte_commandee"):
                r[key] = safe_int(val)
            elif key in ("prix_ha_scc", "pv_resah", "pv_client", "tx_marge", "marge_unitaire"):
                r[key] = safe_float(val)
            else:
                r[key] = safe_str(val)
        records.append(r)

    if not records:
        return False, "Aucun produit trouve."

    with get_db() as conn:
        if mode == "hebdo":
            updated, new = 0, 0
            for r in records:
                ex = conn.execute("SELECT article FROM produits WHERE article=?", (r["article"],)).fetchone()
                if ex:
                    conn.execute("UPDATE produits SET stock_brut=?, updated_at=CURRENT_TIMESTAMP WHERE article=?",
                                 (r["stock_brut"], r["article"]))
                    updated += 1
                else:
                    _insert(conn, r); new += 1
            msg = f"Hebdo : {updated} MAJ"
            if new: msg += f", {new} nouveaux"
            if skipped: msg += f" ({skipped} vides ignorees)"
            return True, msg
        else:
            conn.execute("DELETE FROM produits")
            for r in records: _insert(conn, r)
            msg = f"Import : {len(records)} produits !"
            if skipped: msg += f" ({skipped} vides ignorees)"
            return True, msg

def _insert(conn, r):
    conn.execute("""INSERT OR REPLACE INTO produits
        (article,groupe,code_ic1,vcd,num_projet,nom_projet,ref_fournisseur,libelle,
         marque,affichage,processeur,memoire,stockage,qte_commandee,stock_brut,
         prix_ha_scc,pv_resah,pv_client,tx_marge,marge_unitaire,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
        (r["article"],r.get("groupe",""),r.get("code_ic1",""),r.get("vcd",""),
         r.get("num_projet",""),r.get("nom_projet",""),r.get("ref_fournisseur",""),r["libelle"],
         r.get("marque",""),r.get("affichage",""),r.get("processeur",""),r.get("memoire",""),
         r.get("stockage",""),r.get("qte_commandee",0),r["stock_brut"],
         r.get("prix_ha_scc",0),r.get("pv_resah",0),r.get("pv_client",0),
         r.get("tx_marge",0),r.get("marge_unitaire",0)))

def get_produits():
    return qry("""SELECT p.*,
        p.stock_brut-COALESCE(SUM(CASE WHEN r.statut='actif' THEN r.quantite ELSE 0 END),0) AS stock_disponible,
        COALESCE(SUM(CASE WHEN r.statut='actif' THEN r.quantite ELSE 0 END),0) AS total_reserve
        FROM produits p LEFT JOIN reservations r ON p.article=r.article
        GROUP BY p.article ORDER BY p.marque,p.libelle""")

def get_alertes():
    return qry("""SELECT p.article,p.libelle,p.stock_brut,COALESCE(SUM(r.quantite),0) AS total_reserve
        FROM produits p JOIN reservations r ON p.article=r.article AND r.statut='actif'
        GROUP BY p.article HAVING p.stock_brut<total_reserve""")

def get_reservations(sf=None):
    sql = "SELECT r.*,p.libelle,p.marque FROM reservations r LEFT JOIN produits p ON r.article=p.article"
    p = []
    if sf: sql += " WHERE r.statut=?"; p.append(sf)
    return qry(sql+" ORDER BY r.created_at DESC", tuple(p))

def creer_resa(pers, art, qty, comm, dt):
    prod = qry("SELECT * FROM produits WHERE article=?", (art,), fetch="one")
    if not prod: return False, "Article introuvable."
    res = qry("SELECT COALESCE(SUM(quantite),0) as t FROM reservations WHERE article=? AND statut='actif'", (art,), fetch="one")
    dispo = prod["stock_brut"] - (res["t"] if res else 0)
    if qty > dispo: return False, f"Stock insuffisant ! Dispo:{dispo}, demande:{qty}"
    qry("INSERT INTO reservations(personne,article,quantite,commentaire,date_reservation) VALUES(?,?,?,?,?)",
        (pers, art, qty, comm, dt), fetch="one")
    return True, f"OK : {qty}x {art} pour {pers}"

def upd_resa(rid, st): qry("UPDATE reservations SET statut=? WHERE id=?", (st, rid), fetch="one")
def upd_prod(art, f, v):
    ok = {"ref_fournisseur","libelle","stock_brut","marque","affichage","processeur",
          "memoire","stockage","pv_resah","tx_marge","marge_unitaire","qte_commandee","prix_ha_scc","pv_client"}
    if f in ok: qry(f"UPDATE produits SET {f}=?,updated_at=CURRENT_TIMESTAMP WHERE article=?", (v,art), fetch="one")

# ================================================================
# UI
# ================================================================
st.set_page_config(page_title="StockReserv", page_icon="📦", layout="wide")
st.markdown("""<style>
.stApp{background:#0F1923}
.hb{background:linear-gradient(135deg,#0D1B2A,#1A2D42);border-bottom:3px solid #E88D3F;padding:18px 24px;border-radius:10px;margin-bottom:20px}
.hb h1{color:#E88D3F!important;margin:0!important;font-size:1.7em!important}
.hb p{color:#A0AEC1;margin:4px 0 0}
.ab{background:#3D1F1F;border-left:4px solid #E74C3C;padding:12px 16px;border-radius:0 8px 8px 0;margin:8px 0;color:#F5B7B1}
.sc{background:#1A2D42;border-radius:8px;padding:16px;text-align:center;border:1px solid #2A3F55}
.sc h2{color:#1B998B!important;margin:0!important}.sc p{color:#A0AEC1;margin:4px 0 0}
.rc{border-radius:8px;padding:14px;margin:6px 0;border:1px solid #2A3F55}
</style>""", unsafe_allow_html=True)

init_db()
st.markdown('<div class="hb"><h1>📦 StockReserv</h1><p>Gestion de stock & reservations equipe</p></div>', unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 📂 Import Excel")
    im = st.radio("Type:", ["📥 Premier import (tout)", "🔄 Hebdo (stock seul)"])
    md = "premier" if "Premier" in im else "hebdo"
    up = st.file_uploader("Fichier .xlsx", type=["xlsx","xls"])
    if up:
        ok, msg = import_excel(up, mode=md)
        if ok: st.success(msg)
        else: st.error(msg)
    st.divider()
    st.caption("1er usage → Premier import\nChaque semaine → Hebdo\n(seul Qté Livr/Aff Ligne change)")

alertes = get_alertes()
for a in alertes:
    st.markdown(f'<div class="ab">⚠️ <b>{a["article"]}</b> — {a["libelle"]} : stock({a["stock_brut"]}) &lt; reserves({a["total_reserve"]})</div>', unsafe_allow_html=True)

produits = get_produits()
tr = len(produits)
ts = sum(p["stock_brut"] for p in produits)
tre = sum(p["total_reserve"] for p in produits)
td = sum(max(p["stock_disponible"],0) for p in produits)

c1,c2,c3,c4 = st.columns(4)
with c1: st.markdown(f'<div class="sc"><h2>{tr}</h2><p>Articles</p></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="sc"><h2>{ts}</h2><p>Stock brut</p></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="sc"><h2>{tre}</h2><p>Réservé</p></div>', unsafe_allow_html=True)
with c4: st.markdown(f'<div class="sc"><h2>{td}</h2><p>Disponible</p></div>', unsafe_allow_html=True)

st.markdown("")
t1,t2,t3,t4 = st.tabs(["📦 Produits","✏️ Édition","➕ Réserver","📋 Réservations"])

with t1:
    if not produits: st.info("📂 Importe un Excel via le sidebar.")
    else:
        ca,cb,cc = st.columns([2,1,1])
        with ca: se = st.text_input("🔍 Recherche",key="s1")
        with cb:
            mq = sorted(set(p["marque"] for p in produits if p["marque"]))
            mf = st.selectbox("Marque",["Toutes"]+mq,key="m1")
        with cc: od = st.checkbox("Dispo>0",key="o1")
        df = pd.DataFrame(produits)
        if se:
            s=se.lower()
            df=df[df["article"].astype(str).str.lower().str.contains(s,na=False)|df["libelle"].astype(str).str.lower().str.contains(s,na=False)|df["ref_fournisseur"].astype(str).str.lower().str.contains(s,na=False)|df["vcd"].astype(str).str.lower().str.contains(s,na=False)]
        if mf!="Toutes": df=df[df["marque"]==mf]
        if od: df=df[df["stock_disponible"]>0]
        sh=df[["article","vcd","ref_fournisseur","libelle","marque","processeur","memoire","stockage","affichage","qte_commandee","stock_brut","total_reserve","stock_disponible","pv_resah","tx_marge","marge_unitaire"]].copy()
        sh.columns=["Article","VCD","Réf Fourn.","Libellé","Marque","Processeur","Mémoire","Stockage","Affichage","Qté Cdée","Stock Brut","Réservé","Disponible","PV Resah €","Tx Marge %","Marge €"]
        def cf(v):
            if isinstance(v,(int,float)):
                if v<=0: return "background-color:#5C1A1A;color:#F5B7B1"
                elif v<10: return "background-color:#5C4B1A;color:#F9E79F"
            return ""
        st.dataframe(sh.style.map(cf,subset=["Disponible"]),use_container_width=True,hide_index=True,height=500)
        st.caption(f"{len(sh)} article(s) sur {tr}")

with t2:
    if not produits: st.info("📂 Importe d'abord un Excel.")
    else:
        st.markdown("#### ✏️ Modifie puis sauvegarde")
        edf=pd.DataFrame(produits)
        ed=edf[["article","vcd","ref_fournisseur","libelle","marque","processeur","memoire","stockage","pv_resah","tx_marge","marge_unitaire","stock_brut"]].copy()
        ed.columns=["Article","VCD","Réf Fourn.","Libellé","Marque","Processeur","Mémoire","Stockage","PV Resah €","Tx Marge %","Marge €","Stock Brut"]
        edited=st.data_editor(ed,use_container_width=True,hide_index=True,height=500,num_rows="fixed",disabled=["Article","VCD"],
            column_config={"Stock Brut":st.column_config.NumberColumn(min_value=0,step=1),"PV Resah €":st.column_config.NumberColumn(min_value=0,format="%.2f")},key="pe")
        if st.button("💾 Sauvegarder",type="primary",use_container_width=True):
            cm={"Réf Fourn.":"ref_fournisseur","Libellé":"libelle","Marque":"marque","Processeur":"processeur","Mémoire":"memoire","Stockage":"stockage","PV Resah €":"pv_resah","Tx Marge %":"tx_marge","Marge €":"marge_unitaire","Stock Brut":"stock_brut"}
            ch=0
            for i,row in edited.iterrows():
                a=row["Article"];o=ed.iloc[i]
                for dc,dbc in cm.items():
                    if str(row[dc])!=str(o[dc]): upd_prod(a,dbc,row[dc]);ch+=1
            if ch: st.success(f"{ch} modif(s)!");st.rerun()
            else: st.info("Rien a changer.")

with t3:
    if not produits: st.info("📂 Importe d'abord un Excel.")
    else:
        st.markdown("#### ➕ Réservation")
        c1,c2=st.columns(2)
        with c1:
            prs=st.text_input("👤 Nom",key="rn")
            al=[p["article"] for p in produits]
            asl=st.selectbox("📦 Article",al,format_func=lambda a:f"{a} — {next((p['libelle'][:50] for p in produits if p['article']==a),'')}", key="rv")
        with c2:
            pi=next((p for p in produits if p["article"]==asl),None)
            if pi:
                st.markdown(f"**{pi['libelle']}**\n\n🏷️ {pi['marque']} | VCD:{pi['vcd']}\n\n💰 PV:{pi['pv_resah']}€ | Stock:**{pi['stock_brut']}** | Rés:**{pi['total_reserve']}** | Dispo:**{max(pi['stock_disponible'],0)}**")
                mx=max(pi["stock_disponible"],0)
            else: mx=0
            qt=st.number_input("Quantité",min_value=1,max_value=max(mx,1),value=1,key="rq")
            dr=st.date_input("📅 Date",value=date.today(),key="rd")
        co=st.text_area("💬 Commentaire",key="rc")
        if st.button("✅ Réserver",type="primary",use_container_width=True):
            if not prs.strip(): st.error("Nom requis.")
            else:
                ok,msg=creer_resa(prs.strip(),asl,qt,co.strip(),dr.isoformat())
                if ok: st.success(msg);st.rerun()
                else: st.error(msg)

with t4:
    st.markdown("#### 📋 Réservations")
    fl=st.selectbox("Statut",["Tous","actif","annule","consomme"],key="fs")
    sp=None if fl=="Tous" else fl
    rs=get_reservations(sp)
    if not rs: st.info("Aucune.")
    else:
        for r in rs:
            em={"actif":"🟢","annule":"🔴","consomme":"✅"}.get(r["statut"],"⚪")
            bg={"actif":"#1A3D2A","annule":"#3D1F1F","consomme":"#1A2D42"}.get(r["statut"],"#1A2D42")
            lb={"actif":"Actif","annule":"Annulé","consomme":"Consommé"}.get(r["statut"],r["statut"])
            st.markdown(f'<div class="rc" style="background:{bg}"><strong>{em} #{r["id"]}</strong> — <b>{r["personne"]}</b> → {r["quantite"]}x <code>{r["article"]}</code> ({r.get("libelle","")}) <br/><small>📅 {r["date_reservation"]} | 💬 {r.get("commentaire","") or "—"} | {lb}</small></div>', unsafe_allow_html=True)
            if r["statut"]=="actif":
                b1,b2,_=st.columns([1,1,4])
                with b1:
                    if st.button("✅",key=f"c_{r['id']}"): upd_resa(r["id"],"consomme");st.rerun()
                with b2:
                    if st.button("❌",key=f"a_{r['id']}"): upd_resa(r["id"],"annule");st.rerun()
        st.caption(f"{len(rs)} réservation(s)")
