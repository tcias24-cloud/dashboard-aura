import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Dashboard Aura Lusa", page_icon="🌐", layout="wide")

# --- CSS PERSONALIZADO (Visual Pro) ---
st.markdown("""
    <style>
    .main { background-color: #0E1117; }
    h1, h2, h3 { color: #FAFAFA; font-family: 'Helvetica Neue', sans-serif; }
    .stMetric { background-color: #262730; padding: 15px; border-radius: 5px; border-left: 5px solid #FF4B4B; }
    .stDataFrame { border: 1px solid #444; }
    </style>
""", unsafe_allow_html=True)

# --- FUNÇÃO DE LIMPEZA E TRADUÇÃO (O Segredo) ---
def load_and_clean_data(url, is_weekly=False):
    if not url:
        return None
    
    try:
        # Carregar dados (ignora erros de linhas más)
        df = pd.read_csv(url, on_bad_lines='skip')
        
        # 1. Dicionário de Tradução (Inglês -> Português)
        # Isto resolve o problema dos nomes das colunas
        traducao = {
            'Owner': 'Responsável',
            'Task': 'Tarefa',
            'Status': 'Estado',
            'Priority': 'Prioridade',
            'Prioridade': 'Prioridade', # Caso já esteja em PT
            'Link': 'Link',
            'Deadline': 'Data',
            'Date': 'Data'
        }
        
        # Renomear colunas
        df = df.rename(columns=traducao)
        
        # 2. Garantir que as colunas essenciais existem
        colunas_necessarias = ['Responsável', 'Tarefa', 'Estado', 'Prioridade']
        if is_weekly:
            # Se for semanal, tentamos encontrar uma data, senão criamos hoje
            if 'Data' not in df.columns:
                df['Data'] = datetime.today().strftime('%d/%m/%Y')
            colunas_necessarias.append('Data')
            
        # Verificar se faltam colunas críticas
        missing = [c for c in colunas_necessarias if c not in df.columns]
        if missing:
            st.error(f"⚠️ Colunas em falta no ficheiro: {missing}. Verifique se os títulos na linha 1 do Excel são: Owner, Task, Status, Priority.")
            return None

        # 3. Limpeza Final
        # Selecionar apenas as colunas que interessam (ignora "Point of Contact")
        cols_to_keep = colunas_necessarias + (['Link'] if 'Link' in df.columns else [])
        df = df[cols_to_keep]
        
        # Remover linhas onde não há Tarefa ou Responsável (linhas vazias)
        df = df.dropna(subset=['Tarefa', 'Estado'], how='any')
        
        # Padronizar Texto (Remove espaços extras)
        df['Responsável'] = df['Responsável'].astype(str).str.strip()
        df['Estado'] = df['Estado'].astype(str).str.strip()
        
        return df

    except Exception as e:
        st.error(f"Erro ao ler o ficheiro: {e}")
        return None

# --- BARRA LATERAL ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/9563/9563632.png", width=80)
    st.title("Nexus Control")
    st.markdown("---")
    
    with st.expander("🔗 Fontes de Dados", expanded=True):
        st.info("Cole os links CSV (File > Publish to Web > CSV)")
        url_global = st.text_input("Link Global (CSV)", placeholder="Cole aqui o link Global...")
        url_semanal = st.text_input("Link Semanal (CSV)", placeholder="Cole aqui o link Semanal...")
        
    if st.button("🔄 Sincronizar Agora", type="primary"):
        st.rerun()
    
    st.markdown("---")
    st.caption("v3.0 | Aura Lusa")

# --- LÓGICA PRINCIPAL ---

# 1. Carregar Dados
df_global = load_and_clean_data(url_global, is_weekly=False)
df_weekly = load_and_clean_data(url_semanal, is_weekly=True)

if df_global is None:
    st.warning("👈 Por favor, insira o Link Global na barra lateral para começar.")
    st.stop()

# 2. Filtros Inteligentes
st.header("📊 Visão Geral do Projeto")
col1, col2 = st.columns(2)
with col1:
    filtro_pessoa = st.multiselect("Filtrar por Responsável", options=df_global['Responsável'].unique())
with col2:
    filtro_estado = st.multiselect("Filtrar por Estado", options=df_global['Estado'].unique())

# Aplicar Filtros
df_view = df_global.copy()
if filtro_pessoa:
    df_view = df_view[df_view['Responsável'].isin(filtro_pessoa)]
if filtro_estado:
    df_view = df_view[df_view['Estado'].isin(filtro_estado)]

# 3. Métricas (KPIs)
total = len(df_view)
concluidos = len(df_view[df_view['Estado'].str.contains('Concluído|Feito|Done', case=False, na=False)])
pendentes = total - concluidos
progresso = int((concluidos / total * 100) if total > 0 else 0)

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Total Tarefas", total)
kpi2.metric("Concluídas", concluidos)
kpi3.metric("Progresso Global", f"{progresso}%")

st.progress(progresso / 100)

# 4. Abas Principais
tab1, tab2 = st.tabs(["🌍 Lista Global", "📅 Sprints Semanais"])

with tab1:
    # Cores para a tabela baseadas no estado
    def color_status(val):
        color = 'white'
        if 'Concluído' in val or 'Done' in val: color = '#90EE90' # Verde
        elif 'Andamento' in val or 'Progress' in val: color = '#FFD700' # Amarelo
        elif 'Bloqueado' in val or 'Stuck' in val: color = '#FF6347' # Vermelho
        return f'background-color: {color}; color: black'

    st.subheader("Todas as Tarefas")
    # Mostrar tabela estilizada
    st.dataframe(
        df_view.style.applymap(color_status, subset=['Estado']),
        use_container_width=True,
        hide_index=True
    )

with tab2:
    if df_weekly is not None:
        st.subheader("Histórico Semanal")
        # Gráfico de barras por Semana/Data
        tasks_per_date = df_weekly['Data'].value_counts().reset_index()
        tasks_per_date.columns = ['Data', 'Tarefas']
        
        fig = px.bar(tasks_per_date, x='Data', y='Tarefas', title="Volume de Trabalho por Semana")
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df_weekly, use_container_width=True)
    else:
        st.info("Insira o link Semanal para ver os Sprints.")
