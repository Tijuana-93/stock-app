import streamlit as st
import pandas as pd
import math, requests
from datetime import date

IMPORT_PASSWORD = "admin@123"
TURSO_URL = st.secrets["TURSO_URL"].replace("libsql://","https://")
TURSO_TOKEN = st.secrets["TURSO_TOKEN"]

COL = {
    "article":"Article","groupe":"Groupe","code_ic1":"Code IC1 Ventes","vcd":"VCD",
    "ref_fournisseur":["Réf Fournisseur Principal","Ref Fournisseur Principal"],
    "libelle":["Libelle Complet","Libellé Complet"],"marque":"Marque",
    "affichage":"Affichage","processeur":"Processeur","memoire":["Mémoire","Memoire"],
    "stockage":"Stockage","qte_commandee":["Qté Commandée Ligne","Qte Commandee Ligne"],
    "stock_brut":["Qté Livr/Aff Ligne","Qte Livr/Aff Ligne"],
    "prix_ha_scc":"Prix Unitaire HA SCC","pv_resah":"PV au Resah",
    "pv_client":"PV Client(marge Resah incluse)","tx_marge":"Tx de marge",
    "marge_unitaire":"Montant marge unitaire",
}
REQUIRED_COLUMNS = ["article","libelle","stock_brut"]

def si(v):
    if v is None: return 0
    if isinstance(v,float) and (math.isnan(v) or math.isinf(v)): return 0
    try: return int(float(v))
    except: return 0
def sf(v):
    if v is None: return 0.0
    if isinstance(v,float) and (math.isnan(v) or math.isinf(v)): return 0.0
    try: return float(v)
    except: return 0.0
def ss(v):
    if v is None: return ""
    s=str(v).strip()
    return "" if s.lower() in ("nan","none","nat","") else s

# === TURSO v3 PIPELINE API ===
def _headers():
    return {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}

def _to_arg(v):
    if v is None: return {"type":"null"}
    if isinstance(v,int): return {"type":"integer","value":str(v)}
    if isinstance(v,float): return {"type":"float","value":v}
    return {"type":"text","value":str(v)}

def _extract(cell):
    if cell is None: return None
    if isinstance(cell,dict):
        t=cell.get("type",""); v=cell.get("value")
        if t=="null" or v is None: return None
        if t=="integer": return int(v)
        if t=="float": return float(v)
        return str(v)
    return cell

def turso_exec(sql, args=None):
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [_to_arg(a) for a in args]
    body = {"requests": [
        {"type": "execute", "stmt": stmt},
        {"type": "close"}
    ]}
    resp = requests.post(f"{TURSO_URL}/v3/pipeline", headers=_headers(), json=body)
    if resp.status_code != 200:
        raise Exception(f"Turso HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    results = data.get("results", [])
    if not results: return []
    r0 = results[0]
    if r0.get("type") == "error":
        raise Exception(f"SQL error: {r0.get('error',{}).get('message','unknown')}")
    resp_data = r0.get("response", {}).get("result", {})
    cols = [c["name"] for c in resp_data.get("cols", [])]
    rows = []
    for row in resp_data.get("rows", []):
        rows.append({cols[i]: _extract(cell) for i, cell in enumerate(row)})
    return rows

def turso_run(sql, args=None):
    """Execute sans retour de données"""
    turso_exec(sql, args)

def q(sql, p=None, f="all"):
    rows = turso_exec(sql, p)
    if f=="all": return rows
    if f=="one": return rows[0] if rows else None
    return None

def init():
    turso_run("CREATE TABLE IF NOT EXISTS produits(article TEXT PRIMARY KEY,groupe TEXT DEFAULT '',code_ic1 TEXT DEFAULT '',vcd TEXT DEFAULT '',ref_fournisseur TEXT DEFAULT '',libelle TEXT DEFAULT '',marque TEXT DEFAULT '',affichage TEXT DEFAULT '',processeur TEXT DEFAULT '',memoire TEXT DEFAULT '',stockage TEXT DEFAULT '',qte_commandee INTEGER DEFAULT 0,stock_brut INTEGER DEFAULT 0,prix_ha_scc REAL DEFAULT 0,pv_resah REAL DEFAULT 0,pv_client REAL DEFAULT 0,tx_marge REAL DEFAULT 0,marge_unitaire REAL DEFAULT 0)")
    turso_run("CREATE TABLE IF NOT EXISTS reservations(id INTEGER PRIMARY KEY AUTOINCREMENT,personne TEXT NOT NULL,article TEXT NOT NULL,quantite INTEGER NOT NULL,commentaire TEXT DEFAULT '',date_reservation TEXT DEFAULT '',statut TEXT DEFAULT 'actif')")

def fcol(dfc,names):
    if isinstance(names,str): names=[names]
    for n in names:
        for c in dfc:
            if c.strip().lower()==n.strip().lower(): return c
    return None

def do_import(uf,mode):
    df=pd.read_excel(uf); mp={}
    for k,v in COL.items():
        f=fcol(df.columns,v if isinstance(v,list) else [v])
        if f: mp[k]=f
    for rq in REQUIRED_COLUMNS:
        if rq not in mp: return False,f"Colonne manquante: {rq}"
    recs=[]; skip=0
    for _,row in df.iterrows():
        art=ss(row.get(mp.get("article",""),"")); lib=ss(row.get(mp.get("libelle",""),""))
        if not art or not lib: skip+=1; continue
        r={}
        for k in COL:
            col=mp.get(k,""); val=row.get(col,"") if col else ""
            if k in ("stock_brut","qte_commandee"): r[k]=si(val)
            elif k in ("prix_ha_scc","pv_resah","pv_client","tx_marge","marge_unitaire"): r[k]=sf(val)
            else: r[k]=ss(val)
        recs.append(r)
    if not recs: return False,"Aucun produit."
    if mode=="hebdo":
        u=n=0
        for r in recs:
            ex=q("SELECT 1 FROM produits WHERE article=?",[r["article"]],f="one")
            if ex:
                turso_run("UPDATE produits SET stock_brut=? WHERE article=?",[r["stock_brut"],r["article"]]); u+=1
            else: _ins(r); n+=1
        return True,f"✅ Hebdo: {u} MAJ"+(f", {n} nouveaux" if n else "")
    else:
        turso_run("DELETE FROM produits")
        for r in recs: _ins(r)
        return True,f"✅ {len(recs)} produits importés"

def _ins(r):
    turso_run("INSERT OR REPLACE INTO produits(article,groupe,code_ic1,vcd,ref_fournisseur,libelle,marque,affichage,processeur,memoire,stockage,qte_commandee,stock_brut,prix_ha_scc,pv_resah,pv_client,tx_marge,marge_unitaire) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    [r["article"],r.get("groupe",""),r.get("code_ic1",""),r.get("vcd",""),r.get("ref_fournisseur",""),r["libelle"],r.get("marque",""),r.get("affichage",""),r.get("processeur",""),r.get("memoire",""),r.get("stockage",""),r.get("qte_commandee",0),r["stock_brut"],r.get("prix_ha_scc",0),r.get("pv_resah",0),r.get("pv_client",0),r.get("tx_marge",0),r.get("marge_unitaire",0)])

def get_produits():
    return q("""SELECT p.*,
        p.stock_brut - COALESCE((SELECT SUM(r.quantite) FROM reservations r WHERE r.article=p.article AND r.statut='actif'),0) AS dispo,
        COALESCE((SELECT SUM(r.quantite) FROM reservations r WHERE r.article=p.article AND r.statut='actif'),0) AS reserve
        FROM produits p ORDER BY p.marque,p.libelle""")

def get_reservations(sfv=None):
    if sfv: return q("SELECT r.*,p.libelle,p.marque FROM reservations r LEFT JOIN produits p ON r.article=p.article WHERE r.statut=? ORDER BY r.id DESC",[sfv])
    return q("SELECT r.*,p.libelle,p.marque FROM reservations r LEFT JOIN produits p ON r.article=p.article ORDER BY r.id DESC")

# === UI ===
st.set_page_config(page_title="StockReserv",page_icon="📦",layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
.stApp{background:#F8FAFC}
section[data-testid="stSidebar"]{background:#1E293B!important}
section[data-testid="stSidebar"] *{color:#E2E8F0!important}
section[data-testid="stSidebar"] .stTextInput input{background:#334155!important;color:#F1F5F9!important;border-color:#475569!important}
.hdr{background:linear-gradient(135deg,#1E293B,#334155);padding:28px 36px;border-radius:16px;margin-bottom:24px}
.hdr h1{color:#F8FAFC!important;font-size:2.2em!important;margin:0!important;font-weight:700!important}
.hdr span{color:#F59E0B}.hdr p{color:#94A3B8;margin:8px 0 0;font-size:1.05em}
.kpi{background:white;border-radius:14px;padding:20px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,0.08);border:1px solid #E2E8F0}
.kpi h2{margin:0!important;font-size:2em!important;font-weight:700!important}
.kpi p{color:#64748B;margin:4px 0 0;font-size:.85em;text-transform:uppercase;letter-spacing:.5px}
.kpi.blue h2{color:#3B82F6!important}.kpi.green h2{color:#10B981!important}
.kpi.amber h2{color:#F59E0B!important}.kpi.red h2{color:#EF4444!important}
.alr{background:#FEF2F2;border-left:4px solid #EF4444;padding:12px 16px;border-radius:0 8px 8px 0;margin:8px 0;color:#991B1B}
.rcard{background:white;border-radius:12px;padding:16px 20px;margin:8px 0;border:1px solid #E2E8F0;box-shadow:0 1px 2px rgba(0,0,0,0.04)}
.rcard b{color:#1E293B}.rcard code{background:#F1F5F9;padding:2px 8px;border-radius:4px;color:#0F172A;font-weight:600}
.rcard small{color:#64748B}.rcard.green{border-left:4px solid #10B981}
.rcard.red{border-left:4px solid #EF4444}.rcard.gray{border-left:4px solid #94A3B8}
.stTabs [data-baseweb="tab"]{font-weight:600}
.stTabs [aria-selected="true"]{color:#F59E0B!important;border-bottom-color:#F59E0B!important}
</style>""",unsafe_allow_html=True)

init()
st.markdown('<div class="hdr"><h1>📦 Stock<span>Reserv</span></h1><p>Stock en temps réel · Réservations équipe · Mise à jour hebdo</p></div>',unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 🔐 Admin — Import")
    pwd=st.text_input("Mot de passe",type="password",key="pw")
    if pwd==IMPORT_PASSWORD:
        st.success("Connecté")
        mode=st.radio("Mode:",["📥 Tout charger","🔄 Hebdo (stock seul)"],key="im")
        md="premier" if "Tout" in mode else "hebdo"
        up=st.file_uploader("Excel (.xlsx)",type=["xlsx","xls"],key="fu")
        if up:
            ok,msg=do_import(up,md)
            if ok: st.success(msg)
            else: st.error(msg)
    elif pwd: st.error("Mot de passe incorrect")
    st.divider()
    st.markdown("## 📖 Guide")
    st.markdown("""**🔍 Chercher** → onglet Stock, tape article/libellé/VCD/marque

**➕ Réserver** → onglet Réservation, choisis article + quantité

**📋 Règles** : prénom + nom client dans commentaire
✅ Consommé quand livré · ❌ Annuler si devis perdu""")

prods=get_produits()
for a in [p for p in prods if (p.get("dispo") or 0)<0]:
    st.markdown(f'<div class="alr">⚠️ <b>{a["article"]}</b> {str(a.get("libelle",""))[:40]} — stock insuffisant</div>',unsafe_allow_html=True)

tr=len(prods);ts=sum(p.get("stock_brut",0) or 0 for p in prods)
tre=sum(p.get("reserve",0) or 0 for p in prods);td=sum(max(p.get("dispo",0) or 0,0) for p in prods)
c1,c2,c3,c4=st.columns(4)
with c1:st.markdown(f'<div class="kpi blue"><h2>{tr}</h2><p>Références</p></div>',unsafe_allow_html=True)
with c2:st.markdown(f'<div class="kpi green"><h2>{ts:,}</h2><p>Stock total</p></div>',unsafe_allow_html=True)
with c3:st.markdown(f'<div class="kpi amber"><h2>{tre:,}</h2><p>Réservé</p></div>',unsafe_allow_html=True)
with c4:st.markdown(f'<div class="kpi red"><h2>{td:,}</h2><p>Disponible</p></div>',unsafe_allow_html=True)

st.markdown("")
t1,t2,t3,t4=st.tabs(["📦 Stock & Produits","➕ Nouvelle réservation","📋 Réservations","👥 Par commercial"])

with t1:
    if not prods:st.info("📂 Aucun produit. L'admin doit importer un Excel.")
    else:
        ca,cb,cc=st.columns([3,1,1])
        with ca:se=st.text_input("🔍 Rechercher...",key="s")
        with cb:
            mqs=sorted(set(str(p.get("marque","")) for p in prods if p.get("marque")))
            mf=st.selectbox("Marque",["Toutes"]+mqs,key="mf")
        with cc:od=st.checkbox("En stock uniquement",key="od")
        df=pd.DataFrame(prods)
        if se:
            s=se.lower();df=df[df["article"].astype(str).str.lower().str.contains(s,na=False)|df["libelle"].astype(str).str.lower().str.contains(s,na=False)|df["vcd"].astype(str).str.lower().str.contains(s,na=False)|df["marque"].astype(str).str.lower().str.contains(s,na=False)]
        if mf!="Toutes":df=df[df["marque"]==mf]
        if od:df=df[df["dispo"]>0]
        out=pd.DataFrame()
        out["Article"]=df["article"];out["Marque"]=df["marque"];out["Libellé"]=df["libelle"]
        out["Écran"]=df["affichage"].apply(lambda x:f'{int(sf(x))}"' if sf(x)>0 else "")
        out["Processeur"]=df["processeur"];out["RAM"]=df["memoire"];out["Stockage"]=df["stockage"]
        out["Commandé"]=df["qte_commandee"];out["Stock"]=df["stock_brut"]
        out["Réservé"]=df["reserve"];out["Disponible"]=df["dispo"]
        out["PV Resah"]=df["pv_resah"].apply(lambda x:f"{sf(x):.2f} €")
        out["Marge %"]=df["tx_marge"].apply(lambda x:f"{sf(x)*100:.2f}%")
        out["Marge €"]=df["marge_unitaire"].apply(lambda x:f"{sf(x):.2f} €")
        def rc(row):
            sl=[""]*len(row);idx=row.index.get_loc("Disponible")
            d=(row["Disponible"] or 0);c=(row["Commandé"] or 0)
            if c>0:
                r=d/c
                if r>0.5:sl[idx]="background-color:#D1FAE5;color:#065F46;font-weight:700"
                elif r>0:sl[idx]="background-color:#FEF3C7;color:#92400E;font-weight:700"
                else:sl[idx]="background-color:#FEE2E2;color:#991B1B;font-weight:700"
            else:
                if d>0:sl[idx]="background-color:#D1FAE5;color:#065F46;font-weight:700"
                else:sl[idx]="background-color:#FEE2E2;color:#991B1B;font-weight:700"
            return sl
        st.dataframe(out.style.apply(rc,axis=1),use_container_width=True,hide_index=True,height=550)
        st.caption(f"{len(out)} article(s) sur {tr}")

with t2:
    if not prods:st.info("📂 Aucun produit.")
    else:
        st.markdown("#### Réserver du stock")
        ca,cb=st.columns(2)
        with ca:
            nom=st.text_input("👤 Commercial",placeholder="Romain, Lisa...",key="rn")
            arts=[p["article"] for p in prods]
            sel=st.selectbox("📦 Article",arts,format_func=lambda a:f"{a} — {next((str(p.get('libelle',''))[:50] for p in prods if p['article']==a),'')}", key="ra")
        with cb:
            pi=next((p for p in prods if p["article"]==sel),None)
            if pi:
                d=max(pi.get("dispo",0) or 0,0);c=pi.get("qte_commandee",0) or 0
                pct=f" ({d/c*100:.0f}%)" if c>0 else ""
                cd="🟢" if (c>0 and d/c>0.5) or (c==0 and d>0) else ("🟡" if d>0 else "🔴")
                st.markdown(f"""**{pi['libelle']}**
| | |
|---|---|
|Marque|{pi.get('marque','')}|
|Écran|{int(sf(pi.get('affichage',0)))}"|
|PV Resah|{sf(pi.get('pv_resah',0)):.2f} €|
|Marge|{sf(pi.get('marge_unitaire',0)):.2f} € ({sf(pi.get('tx_marge',0))*100:.2f}%)|
|Commandé|{c}|
|Stock|{pi.get('stock_brut',0)}|
|Réservé|{pi.get('reserve',0)}|
|{cd} Dispo|**{d}**{pct}|""")
                mx=d
            else:mx=0
            qty=st.number_input("Quantité",min_value=1,max_value=max(mx,1),value=1,key="rq")
            dt=st.date_input("Date",value=date.today(),key="rd")
        com=st.text_area("💬 Commentaire",placeholder="Devis client X...",key="rc")
        if st.button("✅ Confirmer",type="primary",use_container_width=True):
            if not nom.strip():st.error("Nom requis")
            else:
                prod=q("SELECT * FROM produits WHERE article=?",[sel],f="one")
                res=q("SELECT COALESCE(SUM(quantite),0) as t FROM reservations WHERE article=? AND statut='actif'",[sel],f="one")
                sd=(prod.get("stock_brut",0) or 0)-((res.get("t",0) or 0) if res else 0)
                if qty>sd:st.error(f"Stock insuffisant! Dispo:{sd}")
                else:
                    turso_run("INSERT INTO reservations(personne,article,quantite,commentaire,date_reservation,statut) VALUES(?,?,?,?,?,'actif')",[nom.strip(),sel,qty,com.strip(),dt.isoformat()])
                    st.success(f"✅ {qty}x {sel} pour {nom.strip()}");st.rerun()

with t3:
    st.markdown("#### Suivi des réservations")
    fl=st.selectbox("Statut",["Tous","actif","annule","consomme"],key="fs",format_func=lambda x:{"Tous":"📊 Tous","actif":"🟢 Actives","annule":"❌ Annulées","consomme":"✅ Consommées"}.get(x,x))
    sp=None if fl=="Tous" else fl;rs=get_reservations(sp)
    if not rs:st.info("Aucune réservation.")
    else:
        for r in rs:
            cls={"actif":"green","annule":"red","consomme":"gray"}.get(str(r.get("statut","")),"")
            em={"actif":"🟢","annule":"❌","consomme":"✅"}.get(str(r.get("statut","")),"")
            lb={"actif":"Active","annule":"Annulée","consomme":"Consommée"}.get(str(r.get("statut","")),"")
            st.markdown(f'<div class="rcard {cls}"><b>{em} {r.get("personne","")}</b> → {r.get("quantite",0)}x <code>{r.get("article","")}</code> · {r.get("libelle","")}<br/><small>📅 {r.get("date_reservation","")} · {r.get("commentaire","") or "—"} · {lb}</small></div>',unsafe_allow_html=True)
            if r.get("statut")=="actif":
                b1,b2,_=st.columns([1,1,5])
                with b1:
                    if st.button("✅",key=f"c{r['id']}"):turso_run("UPDATE reservations SET statut='consomme' WHERE id=?",[r["id"]]);st.rerun()
                with b2:
                    if st.button("❌",key=f"a{r['id']}"):turso_run("UPDATE reservations SET statut='annule' WHERE id=?",[r["id"]]);st.rerun()
        st.caption(f"{len(rs)} réservation(s)")

with t4:
    st.markdown("#### 👥 Par commercial")
    ar=get_reservations()
    if not ar:st.info("Aucune réservation.")
    else:
        df_r=pd.DataFrame(ar);act=df_r[df_r["statut"]=="actif"];con=df_r[df_r["statut"]=="consomme"];ann=df_r[df_r["statut"]=="annule"]
        pers=sorted(df_r["personne"].unique());rows=[]
        for p in pers:
            pa=act[act["personne"]==p];pc=con[con["personne"]==p];pn=ann[ann["personne"]==p]
            ma=mc=0
            for _,r in pa.iterrows():
                pr=next((x for x in prods if x["article"]==r["article"]),None)
                if pr:ma+=r["quantite"]*(pr.get("pv_resah",0) or 0)
            for _,r in pc.iterrows():
                pr=next((x for x in prods if x["article"]==r["article"]),None)
                if pr:mc+=r["quantite"]*(pr.get("pv_resah",0) or 0)
            rows.append({"Commercial":p,"Actives":len(pa),"Qté rés.":int(pa["quantite"].sum()) if len(pa) else 0,"Montant rés.":f"{ma:,.2f} €","Conso.":len(pc),"Qté conso.":int(pc["quantite"].sum()) if len(pc) else 0,"Montant conso.":f"{mc:,.2f} €","Annulées":len(pn)})
        st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)
        st.markdown("---")
        csel=st.selectbox("Détail:",pers,key="cs")
        for _,r in df_r[df_r["personne"]==csel].iterrows():
            em={"actif":"🟢","annule":"❌","consomme":"✅"}.get(str(r.get("statut","")),"")
            lb={"actif":"Active","annule":"Annulée","consomme":"Consommée"}.get(str(r.get("statut","")),"")
            pr=next((x for x in prods if x["article"]==r["article"]),None)
            pv=f" · {r['quantite']*(pr.get('pv_resah',0) or 0):,.2f} €" if pr else ""
            st.markdown(f'{em} {r["quantite"]}x **{r["article"]}** · {r.get("libelle","")}{pv} · {lb} · _{r.get("commentaire","") or "—"}_')
