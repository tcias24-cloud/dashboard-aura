import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import time
import uuid
import requests
from io import StringIO
import re

# --- 1. CONFIGURAÇÃO DO KERNEL (OTIMIZADO) ---
st.set_page_config(
    page_title="Nexus Dashboard | Aura Lusa",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. SISTEMA DE UI/UX (CSS CORPORATIVO) ---
st.markdown("""
    <style>
    /* Reset Global */
    .stApp { background-color: #f8fafc; font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }
    
    /* Cartões de Métricas */
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
        transition: transform 0.2s ease;
    }
    div[data-testid="stMetric"]:hover { transform: translateY(-2px); }
    
    /* Tabelas de Dados */
    .stDataFrame { border: 1px solid #cbd5e1; border-radius: 8px; overflow: hidden; background: white; }
    
    /* Tipografia */
    h1, h2, h3 { color: #0f172a; font-weight: 800; letter-spacing: -0.02em; }
    
    /* Toast de Notificação */
    .stToast { background-color: #ffffff !important; border: 1px solid #e2e8f0; color: #0f172a; }
    </style>
""", unsafe_allow_html=True)

# --- 3. MOTOR ETL UNIVERSAL (EXTRAÇÃO E TRANSFORMAÇÃO) ---

def fetch_data_nuclear(url: str, retries=3) -> pd.DataFrame:
    """
    Motor de Extração Universal Blindado.
    Recupera dados ignorando cache, bloqueios de bot e erros de codificação.
    """
    if not url or not str(url).startswith("http"): return None
    
    clean_url = url.strip()
    
    # Cache Busting Nuclear: UUID garante unicidade absoluta a cada pedido
    sep = '&' if '?' in clean_url else '?'
    bust_url = f"{clean_url}{sep}bust={uuid.uuid4()}"
    
    # Garante formato CSV na Google
    if "docs.google.com" in bust_url and "output=csv" not in bust_url:
        bust_url += "&output=csv"

    # Headers de Browser Real (Evita bloqueio 403)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache'
    }

    for attempt in range(retries):
        try:
            r = requests.get(bust_url, headers=headers, timeout=15)
            r.raise_for_status()
            
            # Validação de Segurança (Content-Type)
            ctype = r.headers.get('Content-Type', '').lower()
            if 'html' in ctype:
                st.sidebar.error("⛔ Erro Crítico: O link devolveu HTML. Use 'Ficheiro > Partilhar > Publicar na Web > CSV'.")
                return None

            content = r.content
            
            # Estratégia de Descodificação em Cascata (UTF-8 -> Latin1 -> CP1252 -> ISO-8859-1)
            # engine='python' e sep=None permitem que o Pandas detete ',' ou ';' automaticamente
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for enc in encodings:
                try:
                    return pd.read_csv(StringIO(content.decode(enc)), sep=None, engine='python', header=None, on_bad_lines='skip')
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            
            return None # Falha total de leitura
        
        except requests.exceptions.RequestException:
            time.sleep(attempt + 1) # Espera exponencial
            continue
        except Exception as e:
            st.sidebar.warning(f"Aviso de leitura: {e}")
            return None
            
    return None

def normalize_and_deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalização Fuzzy com Deduplicação de Colunas.
    Resolve problemas onde o utilizador cria colunas com nomes semelhantes.
    """
    if df.empty: return df
    
    # Limpeza de nomes de colunas
    df.columns = df.columns.astype(str).str.strip()
    col_map = {}
    
    # Mapa de Prioridade (Case Insensitive)
    for col in df.columns:
        c = col.lower()
        if 'owner' in c or 'respons' in c or 'dono' in c: col_map[col] = 'Owner'
        elif 'task' in c or 'tarefa' in c: col_map[col] = 'Task'
        elif 'status' in c or 'estado' in c: col_map[col] = 'Status'
        elif 'prio' in c: col_map[col] = 'Prioridade'
        elif 'link' in c or 'drive' in c or 'pasta' in c: col_map[col] = 'Link'
        elif 'contact' in c or 'contacto' in c: col_map[col] = 'Point of Contact'
        
    df = df.rename(columns=col_map)
    
    # BLINDAGEM: Remove colunas duplicadas mantendo a primeira ocorrência
    # Evita crash do Streamlit ao configurar colunas
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Protocolo de Auto-Cura de Dados."""
    # 1. Normalizar e Deduplicar
    df = normalize_and_deduplicate(df)
    
    # 2. Injetar Colunas em Falta (Estrutura Resiliente)
    required = ['Owner', 'Task', 'Status', 'Prioridade', 'Link', 'Point of Contact']
    for req in required:
        if req not in df.columns: df[req] = np.nan

    # 3. Lógica Merged Cells (Fill Down Inteligente)
    if 'Owner' in df.columns:
        # Garante que vazios, espaços e strings vazias são tratados como NaN
        df['Owner'] = df['Owner'].replace(r'^\s*$', np.nan, regex=True)
        df['Owner'] = df['Owner'].ffill()
        df['Owner'] = df['Owner'].fillna("Geral")

    # 4. Limpeza de Linhas Fantasma
    if 'Task' in df.columns:
        df['Task'] = df['Task'].astype(str).replace('nan', '')
        # Remove tarefas muito curtas (lixo de formatação)
        df = df[df['Task'].str.strip().str.len() > 1]

    # 5. Defaults Seguros (Conversão Forçada para String)
    # Impede erros de comparação se o Excel contiver números nestes campos
    df['Status'] = df['Status'].fillna('Pendente').astype(str)
    df['Prioridade'] = df['Prioridade'].fillna('Normal').astype(str)
    df['Link'] = df['Link'].fillna('').astype(str)
    
    return df

@st.cache_data(ttl=15, show_spinner=False)
def load_global_data(url: str):
    """Carrega dados globais com Scanner Vertical Automático."""
    raw = fetch_data_nuclear(url)
    if raw is None or raw.shape[0] < 2: return pd.DataFrame()
    
    try:
        # Scanner Vertical: Ignora linhas em branco no topo até encontrar o cabeçalho
        header_idx = -1
        for i in range(min(20, len(raw))):
            row_str = raw.iloc[i].astype(str).str.lower().tolist()
            if any('owner' in s or 'respons' in s or 'tarefa' in s for s in row_str):
                header_idx = i
                break
        
        if header_idx == -1: return pd.DataFrame() # Estrutura não reconhecida
        
        headers = raw.iloc[header_idx]
        df = raw.iloc[header_idx+1:].copy()
        df.columns = headers
        return sanitize_dataframe(df)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=15, show_spinner=False)
def load_weekly_data(url: str):
    """Parser Semanal Multi-Bloco com Duplo Scanner (Vertical e Horizontal)."""
    raw = fetch_data_nuclear(url)
    if raw is None or raw.shape[0] < 2: return {}
    
    weeks_map = {}
    try:
        # 1. Encontrar Header Verticalmente
        header_idx = -1
        for i in range(min(20, len(raw))):
            row_str = raw.iloc[i].astype(str).str.lower().tolist()
            if any('owner' in s or 'respons' in s for s in row_str):
                header_idx = i
                break
        
        if header_idx == -1: return {}
        
        # Linha de Datas vs Headers
        # Se header for a linha 0, não há datas acima (caso limite tratado)
        row_dates = raw.iloc[header_idx-1] if header_idx > 0 else pd.Series([np.nan]*len(raw.columns))
        row_headers = raw.iloc[header_idx]
        data_start_idx = header_idx + 1
        
        # 2. Scanner Horizontal para Blocos de Semanas
        starts = [i for i, x in enumerate(row_headers) if 'owner' in str(x).lower() or 'respons' in str(x).lower()]
        
        for start_idx in starts:
            week_name = "Sprint Indefinida"
            # Tenta capturar a data na linha acima
            if start_idx < len(row_dates):
                val = row_dates.iloc[start_idx]
                if pd.notna(val) and str(val).strip() and str(val).lower() != 'nan':
                    week_name = str(val).strip()
            
            # Garante unicidade da chave (Semanas com mesmo nome não se sobrepõem)
            if week_name in weeks_map: week_name += f" ({start_idx})"
            
            # Extrair Bloco (assumindo 6 colunas por semana)
            end_idx = start_idx + 6
            if end_idx <= raw.shape[1]:
                block = raw.iloc[data_start_idx:, start_idx:end_idx].copy()
                block.columns = row_headers.iloc[start_idx:end_idx]
                
                clean = sanitize_dataframe(block)
                if not clean.empty:
                    weeks_map[week_name] = clean
                    
        return weeks_map
    except Exception:
        return {}

# --- 4. MOTOR VISUAL (LÓGICA DE CORES) ---

def apply_styles(df):
    """Aplica formatação condicional baseada em texto."""
    def highlight(row):
        # Converte para string e limpa antes de comparar
        status = str(row.get('Status', '')).lower().strip()
        prio = str(row.get('Prioridade', '')).lower().strip()
        
        # Vermelho (Crítico/Atraso)
        if any(x in status for x in ['atrasado', 'bloqueado', 'cancelado', 'crítico']) or 'crítica' in prio or 'alta' in prio:
            return ['background-color: #fee2e2; color: #991b1b; font-weight: 600'] * len(row)
        # Verde (Sucesso)
        if any(x in status for x in ['feito', 'concluído', 'entregue', 'finalizado', 'done']):
            return ['background-color: #dcfce7; color: #166534'] * len(row)
        # Amarelo (Andamento)
        if any(x in status for x in ['andamento', 'doing', 'progresso', 'iniciado']):
            return ['background-color: #fef9c3; color: #854d0e'] * len(row)
        return [''] * len(row)
    
    return df.style.apply(highlight, axis=1)

# --- 5. ORQUESTRAÇÃO PRINCIPAL ---

def main():
    # Inicialização de Sessão
    if 'url_g' not in st.session_state: st.session_state.url_g = ''
    if 'url_w' not in st.session_state: st.session_state.url_w = ''

    # --- BARRA LATERAL ---
    with st.sidebar:
        st.title("🎛️ Nexus Control")
        st.caption("Aura Lusa | vFinal.GoldenMaster")
        
        with st.expander("🔗 Fontes de Dados", expanded=not st.session_state.url_g):
            st.info("Insira links CSV ('Ficheiro > Partilhar > Publicar na Web').")
            st.session_state.url_g = st.text_input("Link Global (CSV)", st.session_state.url_g).strip()
            st.session_state.url_w = st.text_input("Link Semanal (CSV)", st.session_state.url_w).strip()
            
        if st.button("🔄 Sincronizar Agora", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.toast("A sincronizar com a base de dados...", icon="☁️")
            time.sleep(0.5)
            st.rerun()
            
        if st.session_state.url_g:
            st.success("🟢 Conectado")
            st.markdown(f"<small>Último Sync: {datetime.now().strftime('%H:%M:%S')}</small>", unsafe_allow_html=True)

    # --- VERIFICAÇÃO DE DADOS ---
    if not st.session_state.url_g or not st.session_state.url_w:
        st.info("👋 Bem-vindo. Configure os links CSV na barra lateral para iniciar o sistema.")
        st.stop()

    # --- INGESTÃO ---
    with st.spinner("A processar dados em tempo real..."):
        df_global = load_global_data(st.session_state.url_g)
        weeks_dict = load_weekly_data(st.session_state.url_w)

    if df_global.empty:
        st.error("❌ Erro de Integridade: O ficheiro Global parece vazio ou o link está incorreto.")
        st.stop()

    # --- DASHBOARD ---
    st.title("🚀 Dashboard Operacional")
    
    # Filtros Unificados (Global + Semanal)
    all_owners = set(df_global['Owner'].unique())
    for w in weeks_dict.values(): all_owners.update(w['Owner'].unique())
    
    c1, c2 = st.columns(2)
    with c1: 
        # Remove 'nan' da lista de filtros
        clean_owners = sorted([x for x in all_owners if str(x).lower() != 'nan'])
        sel_owners = st.multiselect("Filtrar Responsável", clean_owners)
    with c2: 
        clean_prios = sorted(df_global['Prioridade'].unique())
        sel_prios = st.multiselect("Filtrar Prioridade", clean_prios)

    def filter_engine(df):
        out = df.copy()
        if sel_owners:
            # Regex match para encontrar nomes parciais (ex: 'Vasco' encontra 'Vasco e Bruno')
            pattern = '|'.join([re.escape(str(o)) for o in sel_owners])
            out = out[out['Owner'].astype(str).str.contains(pattern, case=False, na=False)]
        if sel_prios:
            out = out[out['Prioridade'].isin(sel_prios)]
        return out

    df_view = filter_engine(df_global)

    # KPIs
    st.markdown("### 📊 Métricas de Performance")
    k1, k2, k3, k4 = st.columns(4)
    total = len(df_view)
    # Contagem insensível a maiúsculas/minúsculas
    done = len(df_view[df_view['Status'].str.contains('Feito|Concluído', case=False)])
    late = len(df_view[df_view['Status'].str.contains('Atrasado|Crítica', case=False)])
    
    prog = int((done/total)*100) if total > 0 else 0
    
    k1.metric("Total Tarefas", total)
    k2.metric("Concluídas", done)
    k3.metric("Risco", late, delta="-Alerta" if late > 0 else "OK", delta_color="inverse")
    k4.progress(prog/100, text=f"Progresso Filtrado: {prog}%")

    st.divider()

    # Visualização (Tabs)
    t1, t2 = st.tabs(["🌍 Visão Global", "📅 Sprints Semanais"])
    
    with t1:
        if not df_view.empty:
            st.dataframe(apply_styles(df_view), use_container_width=True, hide_index=True,
                         column_config={"Link": st.column_config.LinkColumn("Drive", display_text="📂")})
        else:
            st.info("Sem dados com os filtros atuais.")

    with t2:
        if weeks_dict:
            w_list = list(weeks_dict.keys())
            target = st.selectbox("Semana:", w_list)
            df_w = filter_engine(weeks_dict[target])
            
            if not df_w.empty:
                st.caption(f"A visualizar: {target} | {len(df_w)} tarefas")
                st.dataframe(apply_styles(df_w), use_container_width=True, hide_index=True,
                             column_config={"Link": st.column_config.LinkColumn("Link", display_text="🔗")})
            else:
                st.warning("Sem tarefas para esta seleção.")
        else:
            st.error("Sem dados semanais detetados. Verifique se o ficheiro tem a estrutura 'Owner/Task' correta.")

if __name__ == "__main__":
    main()
