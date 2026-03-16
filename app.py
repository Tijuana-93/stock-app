"""
StockReserv v6 — UI amelioree, import protege, formats corriges
"""
import streamlit as st
import pandas as pd
import sqlite3
import os
import math
from datetime import date
from contextlib import contextmanager

COLUMN_MAPPING = {
    "article":["Article"],"groupe":["Groupe"],"code_ic1":["Code IC1 Ventes"],
    "vcd":["VCD"],"num_projet":["NUMERO PROJET"],"nom_projet":["Nom du Projet"],
    "ref_fournisseur":["Réf Fournisseur Principal","Ref Fournisseur Principal"],
    "libelle":["Libelle Complet","Libellé Complet"],"marque":["Marque"],
    "affichage":["Affichage"],"processeur":["Processeur"],
    "memoire":["Mémoire","Memoire"],"stockage":["Stockage"],
    "qte_commandee":["Qté Commandée Ligne","Qte Commandee Ligne"],
    "stock_brut":["Qté Livr/Aff Ligne","Qte Livr/Aff Ligne"],
    "prix_ha_scc":["Prix Unitaire HA SCC"],"pv_resah":["PV au Resah"],
    "pv_client":["PV Client(marge Resah incluse)"],
    "tx_marge":["Tx de marge"],"marge_unitaire":["Montant marge unitaire"],
}
REQUIRED_COLUMNS = ["article","libelle","stock_brut"]
IMPORT_PASSWORD = "admin@123"

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "stockreserv.db")

def safe_int(v, d=0):
    if v is None: return d
    if isinstance(v,float) and (math.isnan(v) or math.isinf(v)): return d
    try: return int(float(v))
    except: return d
def safe_float(v, d=0.0):
    if v is None: return d
    if isinstance(v,float) and (math.isnan(v) or math.isinf(v)): return d
    try: return float(v)
    except: return d
def safe_str(v, d=""):
    if v is None: return d
    s = str(v).strip()
    return d if s.lower() in ("nan","none","nat","") else s

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row; conn.execute("PRAGMA journal_mode=WAL")
    try: yield conn; conn.commit()
    except: conn.rollback(); raise
    finally: conn.close()

def init_db():
    if os.path.exists(DB_PATH):
        try:
            c = sqlite3.connect(DB_PATH); cols = [r[1] for r in c.execute("PRAGMA table_info(produits)").fetchall()]; c.close()
            if cols and "article" not in cols: os.remove(DB_PATH)
        except: 
            try: os.remove(DB_PATH)
            except: pass
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS produits (
                article TEXT PRIMARY KEY, groupe TEXT DEFAULT '', code_ic1 TEXT DEFAULT '',
                vcd TEXT DEFAULT '', num_projet TEXT DEFAULT '', nom_projet TEXT DEFAULT '',
                ref_fournisseur TEXT DEFAULT '', libelle TEXT DEFAULT '', marque TEXT DEFAULT '',
                affichage TEXT DEFAULT '', processeur TEXT DEFAULT '', memoire TEXT DEFAULT '',
                stockage TEXT DEFAULT '', qte_commandee INTEGER DEFAULT 0, stock_brut INTEGER DEFAULT 0,
                prix_ha_scc REAL DEFAULT 0, pv_resah REAL DEFAULT 0, pv_client REAL DEFAULT 0,
                tx_marge REAL DEFAULT 0, marge_unitaire REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, personne TEXT NOT NULL, article TEXT NOT NULL,
                quantite INTEGER NOT NULL, commentaire TEXT DEFAULT '',
                date_reservation DATE DEFAULT CURRENT_DATE,
                statut TEXT CHECK(statut IN ('actif','annule','consomme')) DEFAULT 'actif',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (article) REFERENCES produits(article));""")

def qry(sql, params=(), fetch="all"):
    with get_db() as conn:
        cur = conn.execute(sql, params)
        if fetch=="all": return [dict(r) for r in cur.fetchall()]
        elif fetch=="one":
            r=cur.fetchone(); return dict(r) if r else None
        return cur.lastrowid

def find_column(dfc, pn):
    for n in pn:
        for c in dfc:
            if c.strip().lower()==n.strip().lower(): return c
    return None

def import_excel(uf, mode="premier"):
    df = pd.read_excel(uf); mapping = {}
    for k,p in COLUMN_MAPPING.items():
        f=find_column(df.columns,p)
        if f: mapping[k]=f
    missing=[k for k in REQUIRED_COLUMNS if k not in mapping]
    if missing: return False,f"Colonnes introuvables: {missing}\nDetectees: {list(df.columns)}"
    records=[]; skipped=0
    for _,row in df.iterrows():
        art=safe_str(row.get(mapping.get("article",""),""))
        lib=safe_str(row.get(mapping.get("libelle",""),""))
        if not art or not lib: skipped+=1; continue
        r={}
        for k in COLUMN_MAPPING:
            col=mapping.get(k,""); val=row.get(col,"") if col else ""
            if k in ("stock_brut","qte_commandee"): r[k]=safe_int(val)
            elif k in ("prix_ha_scc","pv_resah","pv_client","tx_marge","marge_unitaire"): r[k]=safe_float(val)
            else: r[k]=safe_str(val)
        records.append(r)
    if not records: return False,"Aucun produit."
    with get_db() as conn:
        if mode=="hebdo":
            u,n=0,0
            for r in records:
                if conn.execute("SELECT article FROM produits WHERE article=?",(r["article"],)).fetchone():
                    conn.execute("UPDATE produits SET stock_brut=?,updated_at=CURRENT_TIMESTAMP WHERE article=?",(r["stock_brut"],r["article"])); u+=1
                else: _ins(conn,r); n+=1
            msg=f"Hebdo: {u} MAJ"; 
            if n: msg+=f", {n} nouveaux"
            if skipped: msg+=f" ({skipped} vides)"
            return True,msg
        else:
            conn.execute("DELETE FROM produits")
            for r in records: _ins(conn,r)
            msg=f"Import: {len(records)} produits!"
            if skipped: msg+=f" ({skipped} vides)"
            return True,msg

def _ins(conn,r):
    conn.execute("""INSERT OR REPLACE INTO produits(article,groupe,code_ic1,vcd,num_projet,nom_projet,
        ref_fournisseur,libelle,marque,affichage,processeur,memoire,stockage,qte_commandee,stock_brut,
        prix_ha_scc,pv_resah,pv_client,tx_marge,marge_unitaire,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
        (r["article"],r.get("groupe",""),r.get("code_ic1",""),r.get("vcd",""),r.get("num_projet",""),
         r.get("nom_projet",""),r.get("ref_fournisseur",""),r["libelle"],r.get("marque",""),
         r.get("affichage",""),r.get("processeur",""),r.get("memoire",""),r.get("stockage",""),
         r.get("qte_commandee",0),r["stock_brut"],r.get("prix_ha_scc",0),r.get("pv_resah",0),
         r.get("pv_client",0),r.get("tx_marge",0),r.get("marge_unitaire",0)))

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
    sql="SELECT r.*,p.libelle,p.marque FROM reservations r LEFT JOIN produits p ON r.article=p.article"
    p=[]
    if sf: sql+=" WHERE r.statut=?"; p.append(sf)
    return qry(sql+" ORDER BY r.created_at DESC",tuple(p))

def creer_resa(pers,art,qty,comm,dt):
    prod=qry("SELECT * FROM produits WHERE article=?",(art,),fetch="one")
    if not prod: return False,"Article introuvable."
    res=qry("SELECT COALESCE(SUM(quantite),0) as t FROM reservations WHERE article=? AND statut='actif'",(art,),fetch="one")
    dispo=prod["stock_brut"]-(res["t"] if res else 0)
    if qty>dispo: return False,f"Stock insuffisant! Dispo:{dispo}"
    qry("INSERT INTO reservations(personne,article,quantite,commentaire,date_reservation) VALUES(?,?,?,?,?)",(pers,art,qty,comm,dt),fetch="one")
    return True,f"OK: {qty}x {art} pour {pers}"

def upd_resa(rid,s): qry("UPDATE reservations SET statut=? WHERE id=?",(s,rid),fetch="one")
def upd_prod(art,f,v):
    ok={"ref_fournisseur","libelle","stock_brut","marque","affichage","processeur","memoire","stockage","pv_resah","tx_marge","marge_unitaire","qte_commandee","prix_ha_scc","pv_client"}
    if f in ok: qry(f"UPDATE produits SET {f}=?,updated_at=CURRENT_TIMESTAMP WHERE article=?",(v,art),fetch="one")

def format_affichage(val):
    """Ajoute 'pouces' si c'est un nombre"""
    v = safe_str(val)
    if not v: return ""
    return f'{v}"' if v else ""

# ================================================================
# UI
# ================================================================
st.set_page_config(page_title="StockReserv", page_icon="📦", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Space+Mono:wght@700&display=swap');

.stApp { background: #111827; font-family: 'DM Sans', sans-serif; }

/* Header */
.hdr {
    background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
    border-left: 5px solid #F59E0B;
    padding: 24px 32px; border-radius: 0 16px 16px 0;
    margin-bottom: 28px;
}
.hdr h1 { color: #F5F5F5 !important; font-family: 'Space Mono', monospace !important; font-size: 2em !important; margin: 0 !important; letter-spacing: -1px; }
.hdr span { color: #F59E0B; }
.hdr p { color: #94A3B8; margin: 6px 0 0; font-size: 1em; }

/* Alert */
.alr { background: #451A1A; border-left: 4px solid #EF4444; padding: 14px 18px; border-radius: 0 10px 10px 0; margin: 8px 0; color: #FCA5A5; font-size: 0.95em; }

/* Stat cards */
.stc {
    background: linear-gradient(145deg, #1E293B, #1A2332);
    border-radius: 14px; padding: 22px 16px; text-align: center;
    border: 1px solid #334155; transition: transform 0.2s;
}
.stc:hover { transform: translateY(-2px); }
.stc h2 { color: #F5F5F5 !important; font-family: 'Space Mono', monospace !important; font-size: 2em !important; margin: 0 !important; }
.stc p { color: #94A3B8; margin: 6px 0 0; font-size: 0.9em; text-transform: uppercase; letter-spacing: 1px; }
.stc.green h2 { color: #34D399 !important; }
.stc.amber h2 { color: #F59E0B !important; }
.stc.red h2 { color: #EF4444 !important; }
.stc.blue h2 { color: #60A5FA !important; }

/* Reservation cards */
.rcard { border-radius: 12px; padding: 16px 20px; margin: 8px 0; border: 1px solid #334155; }
.rcard b { color: #F5F5F5; }
.rcard code { background: #334155; padding: 2px 8px; border-radius: 4px; color: #F59E0B; }
.rcard small { color: #94A3B8; }

/* Sidebar */
section[data-testid="stSidebar"] { background: #0F172A !important; }
section[data-testid="stSidebar"] .stMarkdown h3 { color: #F5F5F5 !important; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] { color: #94A3B8 !important; font-weight: 500; }
.stTabs [aria-selected="true"] { color: #F59E0B !important; }

/* Buttons */
.stButton > button { border-radius: 8px !important; font-weight: 600 !important; }
.stButton > button[kind="primary"] { background: #F59E0B !important; color: #0F172A !important; border: none !important; }
.stButton > button[kind="primary"]:hover { background: #D97706 !important; }

/* Inputs */
.stTextInput > div > div > input, .stTextArea > div > div > textarea { 
    background: #1E293B !important; color: #F5F5F5 !important; border-color: #334155 !important; border-radius: 8px !important; }
.stSelectbox > div > div { background: #1E293B !important; color: #F5F5F5 !important; border-radius: 8px !important; }

/* Dataframe */
.stDataFrame { border-radius: 12px; overflow: hidden; }

/* General text */
.stMarkdown p, .stMarkdown li { color: #CBD5E1; }
.stMarkdown h4 { color: #F5F5F5 !important; }
.stCaption p { color: #64748B !important; }
</style>""", unsafe_allow_html=True)

init_db()

st.markdown('<div class="hdr"><h1>📦 Stock<span>Reserv</span></h1><p>Gestion de stock & réservations équipe — partagez le lien</p></div>', unsafe_allow_html=True)

# -- Sidebar --
with st.sidebar:
    st.markdown("### 🔐 Import Excel")
    pwd = st.text_input("Mot de passe admin", type="password", key="pwd")
    admin_ok = pwd == IMPORT_PASSWORD

    if admin_ok:
        st.success("✅ Admin connecté")
        im = st.radio("Type:", ["📥 Premier import (tout)", "🔄 Hebdo (stock seul)"])
        md = "premier" if "Premier" in im else "hebdo"
        up = st.file_uploader("Fichier .xlsx", type=["xlsx","xls"])
        if up:
            ok, msg = import_excel(up, mode=md)
            if ok: st.success(msg)
            else: st.error(msg)
    elif pwd:
        st.error("❌ Mot de passe incorrect")
    else:
        st.caption("Entrez le mot de passe pour importer")

    st.divider()
    st.markdown("### ℹ️ Aide")
    st.caption("• 1er usage → Premier import\n• Chaque semaine → Hebdo\n  (seul Qté Livr/Aff change)\n• Onglet Réserver → nouvelle résa\n• Onglet Réservations → gérer")

# -- Data --
alertes = get_alertes()
for a in alertes:
    st.markdown(f'<div class="alr">⚠️ <b>{a["article"]}</b> — {a["libelle"]} : stock({a["stock_brut"]}) &lt; réservé({a["total_reserve"]})</div>', unsafe_allow_html=True)

produits = get_produits()
tr=len(produits); ts=sum(p["stock_brut"] for p in produits)
tre=sum(p["total_reserve"] for p in produits); td=sum(max(p["stock_disponible"],0) for p in produits)

c1,c2,c3,c4 = st.columns(4)
with c1: st.markdown(f'<div class="stc blue"><h2>{tr}</h2><p>Articles</p></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="stc green"><h2>{ts:,}</h2><p>Stock brut</p></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="stc amber"><h2>{tre:,}</h2><p>Réservé</p></div>', unsafe_allow_html=True)
with c4: st.markdown(f'<div class="stc red"><h2>{td:,}</h2><p>Disponible</p></div>', unsafe_allow_html=True)

st.markdown("")
t1,t2,t3,t4 = st.tabs(["📦 Produits & Stock","✏️ Édition Live","➕ Réserver","📋 Réservations"])

# --- PRODUITS ---
with t1:
    if not produits: st.info("📂 Aucun produit. L'admin doit importer un Excel.")
    else:
        ca,cb,cc = st.columns([2,1,1])
        with ca: se=st.text_input("🔍 Recherche (article, libellé, VCD, réf fourn.)",key="s1")
        with cb:
            mqs=sorted(set(p["marque"] for p in produits if p["marque"]))
            mf=st.selectbox("Marque",["Toutes"]+mqs,key="m1")
        with cc: od=st.checkbox("Dispo > 0 uniquement",key="o1")

        df=pd.DataFrame(produits)
        if se:
            s=se.lower()
            df=df[df["article"].astype(str).str.lower().str.contains(s,na=False)|df["libelle"].astype(str).str.lower().str.contains(s,na=False)|df["ref_fournisseur"].astype(str).str.lower().str.contains(s,na=False)|df["vcd"].astype(str).str.lower().str.contains(s,na=False)]
        if mf!="Toutes": df=df[df["marque"]==mf]
        if od: df=df[df["stock_disponible"]>0]

        # Build display dataframe with formatting
        sh = pd.DataFrame()
        sh["Article"] = df["article"]
        sh["VCD"] = df["vcd"]
        sh["Réf Fourn."] = df["ref_fournisseur"]
        sh["Libellé"] = df["libelle"]
        sh["Marque"] = df["marque"]
        sh["Processeur"] = df["processeur"]
        sh["Mémoire"] = df["memoire"]
        sh["Stockage"] = df["stockage"]
        sh["Affichage"] = df["affichage"].apply(lambda x: f'{safe_str(x)}"' if safe_str(x) else "")
        sh["Qté Cdée"] = df["qte_commandee"]
        sh["Stock Brut"] = df["stock_brut"]
        sh["Réservé"] = df["total_reserve"]
        sh["Disponible"] = df["stock_disponible"]
        sh["PV Resah €"] = df["pv_resah"].apply(lambda x: f"{x:,.2f} €".replace(",", " "))
        sh["Tx Marge %"] = df["tx_marge"].apply(lambda x: f"{x*100:.2f} %")
        sh["Marge Resah €"] = df["marge_unitaire"].apply(lambda x: f"{x:,.2f} €".replace(",", " "))

        # Color function: compare disponible vs commandee
        def color_dispo(row):
            styles = [""] * len(row)
            dispo_idx = row.index.get_loc("Disponible")
            cdee_idx = row.index.get_loc("Qté Cdée")
            dispo = row["Disponible"]
            cdee = row["Qté Cdée"]
            if cdee > 0:
                ratio = dispo / cdee
                if ratio > 0.5:
                    styles[dispo_idx] = "background-color: #064E3B; color: #6EE7B7; font-weight: 700"
                elif ratio > 0:
                    styles[dispo_idx] = "background-color: #78350F; color: #FCD34D; font-weight: 700"
                else:
                    styles[dispo_idx] = "background-color: #7F1D1D; color: #FCA5A5; font-weight: 700"
            else:
                if dispo > 0:
                    styles[dispo_idx] = "background-color: #064E3B; color: #6EE7B7; font-weight: 700"
                else:
                    styles[dispo_idx] = "background-color: #7F1D1D; color: #FCA5A5; font-weight: 700"
            return styles

        styled = sh.style.apply(color_dispo, axis=1)
        st.dataframe(styled, use_container_width=True, hide_index=True, height=550)
        st.caption(f"📊 {len(sh)} article(s) sur {tr}")

# --- EDITION ---
with t2:
    if not produits: st.info("📂 Importe d'abord un Excel.")
    else:
        st.markdown("#### ✏️ Clique sur une cellule, modifie, puis sauvegarde")
        edf=pd.DataFrame(produits)
        ed=edf[["article","vcd","ref_fournisseur","libelle","marque","processeur","memoire","stockage","pv_resah","tx_marge","marge_unitaire","stock_brut"]].copy()
        # Convertir tx_marge en % pour l'edition
        ed["tx_marge"] = ed["tx_marge"] * 100
        ed.columns=["Article","VCD","Réf Fourn.","Libellé","Marque","Processeur","Mémoire","Stockage","PV Resah €","Tx Marge %","Marge Resah €","Stock Brut"]
        edited=st.data_editor(ed,use_container_width=True,hide_index=True,height=500,num_rows="fixed",disabled=["Article","VCD"],
            column_config={
                "Stock Brut":st.column_config.NumberColumn(min_value=0,step=1),
                "PV Resah €":st.column_config.NumberColumn(min_value=0,format="%.2f €"),
                "Tx Marge %":st.column_config.NumberColumn(format="%.2f %%"),
                "Marge Resah €":st.column_config.NumberColumn(format="%.2f €"),
            },key="pe")
        if st.button("💾 Sauvegarder les modifications",type="primary",use_container_width=True):
            cm={"Réf Fourn.":"ref_fournisseur","Libellé":"libelle","Marque":"marque","Processeur":"processeur","Mémoire":"memoire","Stockage":"stockage","PV Resah €":"pv_resah","Tx Marge %":"tx_marge","Marge Resah €":"marge_unitaire","Stock Brut":"stock_brut"}
            ch=0
            for i,row in edited.iterrows():
                a=row["Article"]; o=ed.iloc[i]
                for dc,dbc in cm.items():
                    if str(row[dc])!=str(o[dc]):
                        val = row[dc]
                        # Reconvertir tx_marge de % vers decimal pour stockage
                        if dbc == "tx_marge": val = val / 100
                        upd_prod(a,dbc,val); ch+=1
            if ch: st.success(f"✅ {ch} modification(s) sauvegardée(s)"); st.rerun()
            else: st.info("Aucune modification détectée.")

# --- RESERVER ---
with t3:
    if not produits: st.info("📂 Aucun produit chargé.")
    else:
        st.markdown("#### ➕ Nouvelle réservation de stock")
        c1,c2=st.columns(2)
        with c1:
            prs=st.text_input("👤 Nom du commercial",placeholder="Romain, Lisa...",key="rn")
            al=[p["article"] for p in produits]
            asl=st.selectbox("📦 Article",al,format_func=lambda a:f"{a} — {next((p['libelle'][:50] for p in produits if p['article']==a),'')}", key="rv")
        with c2:
            pi=next((p for p in produits if p["article"]==asl),None)
            if pi:
                dispo = max(pi['stock_disponible'],0)
                cdee = pi['qte_commandee']
                pct = f"({dispo/cdee*100:.0f}%)" if cdee > 0 else ""
                st.markdown(f"""**{pi['libelle']}**  
🏷️ {pi['marque']} | VCD: {pi['vcd']}  
💰 PV Resah: {pi['pv_resah']:,.2f} € | Marge: {pi['marge_unitaire']:,.2f} €  
📦 Commandé: **{cdee}** | Stock: **{pi['stock_brut']}** | Réservé: **{pi['total_reserve']}** | Dispo: **{dispo}** {pct}""")
                mx=dispo
            else: mx=0
            qt=st.number_input("Quantité à réserver",min_value=1,max_value=max(mx,1),value=1,key="rq")
            dr=st.date_input("📅 Date de réservation",value=date.today(),key="rd")
        co=st.text_area("💬 Commentaire",placeholder="Devis client X, PO en attente...",key="rc")
        if st.button("✅ Créer la réservation",type="primary",use_container_width=True):
            if not prs.strip(): st.error("❌ Indique le nom du commercial.")
            else:
                ok,msg=creer_resa(prs.strip(),asl,qt,co.strip(),dr.isoformat())
                if ok: st.success(f"✅ {msg}"); st.rerun()
                else: st.error(f"❌ {msg}")

# --- RESERVATIONS ---
with t4:
    st.markdown("#### 📋 Suivi des réservations")
    fl=st.selectbox("Filtrer par statut",["Tous","actif","annule","consomme"],key="fs",
                    format_func=lambda x: {"Tous":"📊 Tous","actif":"🟢 Actifs","annule":"🔴 Annulés","consomme":"✅ Consommés"}.get(x,x))
    sp=None if fl=="Tous" else fl
    rs=get_reservations(sp)
    if not rs: st.info("Aucune réservation enregistrée.")
    else:
        for r in rs:
            em={"actif":"🟢","annule":"🔴","consomme":"✅"}.get(r["statut"],"⚪")
            bg={"actif":"#0D2818","annule":"#2D0F0F","consomme":"#1E293B"}.get(r["statut"],"#1E293B")
            lb={"actif":"Actif","annule":"Annulé","consomme":"Consommé"}.get(r["statut"],r["statut"])
            st.markdown(f'<div class="rcard" style="background:{bg}"><strong>{em} #{r["id"]}</strong> — <b>{r["personne"]}</b> → {r["quantite"]}x <code>{r["article"]}</code> ({r.get("libelle","")}) <br/><small>📅 {r["date_reservation"]} | 💬 {r.get("commentaire","") or "—"} | <b>{lb}</b></small></div>', unsafe_allow_html=True)
            if r["statut"]=="actif":
                b1,b2,_=st.columns([1,1,4])
                with b1:
                    if st.button("✅ Consommé",key=f"c_{r['id']}"): upd_resa(r["id"],"consomme"); st.rerun()
                with b2:
                    if st.button("❌ Annuler",key=f"a_{r['id']}"): upd_resa(r["id"],"annule"); st.rerun()
        st.caption(f"📊 {len(rs)} réservation(s)")
