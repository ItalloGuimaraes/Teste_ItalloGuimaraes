import os
import zipfile
import pandas as pd
import glob
import logging
import shutil
import requests
import io
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Configuração de Log.
logging.basicConfig(
    filename='relatorio_inconsistencias.txt', 
    level=logging.INFO, 
    format='%(asctime)s - %(message)s',
    encoding='utf-8'
)

RAW_DIR = "data/raw"
EXTRACTED_DIR = "data/extracted"
AUX_DIR = "data/auxiliary" 
OUTPUT_FILE = "consolidado_despesas.csv"

CADOP_DIR_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"

# Busca dinâmica do arquivo CADOP.
def find_cadop_url():
    print(f"  -> Acessando diretório: {CADOP_DIR_URL}...")
    try:
        response = requests.get(CADOP_DIR_URL, verify=False, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = [a.get('href') for a in soup.find_all('a') if a.get('href')]
        candidates = [l for l in links if ('csv' in l.lower() or 'zip' in l.lower()) and ('cadop' in l.lower() or 'relatorio' in l.lower())]
        
        if candidates:
            best_match = candidates[0]
            print(f"     [SUCESSO] Arquivo encontrado: {best_match}")
            return urljoin(CADOP_DIR_URL, best_match), best_match
            
    except Exception as e:
        print(f"     [AVISO] Erro ao listar diretório: {e}")
    
    return urljoin(CADOP_DIR_URL, "Relatorio_cadop.csv"), "Relatorio_cadop.csv"

def download_and_extract_cadop():
    os.makedirs(AUX_DIR, exist_ok=True)
    cadop_url, filename = find_cadop_url()
    local_csv_path = os.path.join(AUX_DIR, "Relatorio_Cadop.csv")
    
    if os.path.exists(local_csv_path) and os.path.getsize(local_csv_path) > 1024:
        return local_csv_path

    print(f"  -> Baixando {filename}...")
    try:
        response = requests.get(cadop_url, verify=False, timeout=60)
        response.raise_for_status()
        
        if filename.lower().endswith('.zip'):
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
                if csv_files:
                    with z.open(csv_files[0]) as zf, open(local_csv_path, 'wb') as f:
                        shutil.copyfileobj(zf, f)
                    return local_csv_path
        else:
            with open(local_csv_path, 'wb') as f:
                f.write(response.content)
            return local_csv_path
    except Exception as e:
        print(f"     [AVISO] Falha no download do CADOP: {e}")
        return None

def load_cadop_mapping():
    cadop_path = download_and_extract_cadop()
    if not cadop_path: return None

    # Tenta ler com diferentes encodings
    encodings_to_try = ['utf-8', 'latin1', 'cp1252']
    
    for encoding in encodings_to_try:
        try:
            df_cadop = pd.read_csv(cadop_path, sep=';', encoding=encoding, dtype=str, on_bad_lines='skip', quotechar='"')
            df_cadop.columns = df_cadop.columns.str.strip().str.upper()
            
            reg_col = next((c for c in df_cadop.columns if 'REGISTRO' in c and 'DATA' not in c), None)
            
            if reg_col:
                cnpj_col = next((c for c in df_cadop.columns if 'CNPJ' in c), None)
                raz_col = next((c for c in df_cadop.columns if 'RAZAO' in c), None)

                if reg_col and cnpj_col and raz_col:
                    df_cadop[reg_col] = df_cadop[reg_col].astype(str).str.replace('"', '').str.strip()
                    return df_cadop.set_index(reg_col)[[cnpj_col, raz_col]].to_dict('index')
        except Exception:
            continue
    return None

def extract_files():
    print(">>> Iniciando extração...")
    if os.path.exists(EXTRACTED_DIR): shutil.rmtree(EXTRACTED_DIR)
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    zips = glob.glob(os.path.join(RAW_DIR, "*.zip"))
    for zip_path in zips:
        try:
            with zipfile.ZipFile(zip_path, 'r') as z: z.extractall(EXTRACTED_DIR)
        except: pass

def normalize_columns(df):
    df.columns = df.columns.str.strip().str.upper()
    column_map = {
        'NR_CNPJ': 'CNPJ', 'CNPJ': 'CNPJ',
        'NM_RAZAO_SOCIAL': 'RazaoSocial', 'RAZAO_SOCIAL': 'RazaoSocial',
        'REG_ANS': 'RegAns', 
        'DATA': 'Data', 'DT_FIM_EXERCICIO': 'Data',
        'VL_SALDO_FINAL': 'Valor Despesas', 'VALOR': 'Valor Despesas',
        'DESCRICAO': 'Descricao', 'CD_CONTA_CONTABIL': 'Conta'
    }
    df = df.rename(columns=column_map)
    
    if 'RegAns' in df.columns:
        df['RegAns'] = df['RegAns'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.replace('"', '')

    if 'Data' in df.columns and 'Trimestre' not in df.columns:
        #Se a conversão falhar (coerce), vira NaT. 
        df['Data_Original'] = df['Data'] # Guarda para logar depois
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Trimestre'] = df['Data'].dt.quarter
        df['Ano'] = df['Data'].dt.year
    return df

def clean_and_validate(df, filename, mapping_cadop):
    if 'Descricao' in df.columns:
        mask = df['Descricao'].astype(str).str.contains('EVENTO|SINISTRO', case=False, na=False)
        df_filtered = df[mask].copy()
        if df_filtered.empty: return pd.DataFrame()
    else: return pd.DataFrame()

    if 'CNPJ' not in df_filtered.columns: df_filtered['CNPJ'] = None
    if 'RazaoSocial' not in df_filtered.columns: df_filtered['RazaoSocial'] = None

    if mapping_cadop and 'RegAns' in df_filtered.columns:
        def get_info(reg):
            val = mapping_cadop.get(reg)
            if not val: val = mapping_cadop.get(reg.lstrip('0'))
            if not val: val = mapping_cadop.get(reg.zfill(6))
            return val if val else {}

        cadop_data = df_filtered['RegAns'].apply(get_info)
        df_filtered['CNPJ'] = df_filtered['CNPJ'].fillna(cadop_data.apply(lambda x: x.get('CNPJ')))
        df_filtered['RazaoSocial'] = df_filtered['RazaoSocial'].fillna(
            cadop_data.apply(lambda x: x.get('RAZAO_SOCIAL') or x.get('Razao_Social'))
        )

    if 'RegAns' in df_filtered.columns:
        df_filtered['CNPJ'] = df_filtered['CNPJ'].fillna("Reg: " + df_filtered['RegAns'].astype(str))
        df_filtered['RazaoSocial'] = df_filtered['RazaoSocial'].fillna("ID: " + df_filtered['RegAns'].astype(str))

    # Tratamento Numérico
    if 'Valor Despesas' in df_filtered.columns:
        if not pd.api.types.is_numeric_dtype(df_filtered['Valor Despesas']):
            df_filtered['Valor Despesas'] = (
                df_filtered['Valor Despesas'].astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
            )
            df_filtered['Valor Despesas'] = pd.to_numeric(df_filtered['Valor Despesas'], errors='coerce')
        df_filtered['Valor Despesas'] = df_filtered['Valor Despesas'].fillna(0.0)

    # INCONSISTÊNCIAS: VALORES E DATAS (Por arquivo)
    # 1. Valores Negativos
    negativos = df_filtered[df_filtered['Valor Despesas'] < 0]
    if not negativos.empty: 
        logging.warning(f"{filename}: {len(negativos)} registros com VALOR NEGATIVO (mantidos).")
    
    # 2. Valores Zerados 
    zerados = df_filtered[df_filtered['Valor Despesas'] == 0]
    if not zerados.empty:
        logging.warning(f"{filename}: {len(zerados)} registros com VALOR ZERADO (mantidos).")

    # 3. Inconsistência de Data
    if 'Trimestre' in df_filtered.columns:
        datas_inv = df_filtered[df_filtered['Trimestre'].isna()]
        if not datas_inv.empty:
            exemplos = datas_inv['Data_Original'].unique()[:3]
            logging.warning(f"{filename}: {len(datas_inv)} registros com DATA INVALIDA/INCONSISTENTE. Exemplos: {exemplos}")

    cols = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'Valor Despesas']
    for c in cols: 
        if c not in df_filtered.columns: df_filtered[c] = None
    return df_filtered[cols]

def process_data():
    extract_files()
    mapping = load_cadop_mapping()
    all_data = []
    files = glob.glob(os.path.join(EXTRACTED_DIR, "**/*.csv"), recursive=True)
    
    print(f"\n>>> Processando {len(files)} arquivos...")
    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"  -> Lendo: {filename}...", end=" ")
        try:
            try: df = pd.read_csv(file_path, sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)
            except: df = pd.read_csv(file_path, sep=';', encoding='latin1', on_bad_lines='skip', dtype=str)
            
            print(f"[OK] {len(df)} linhas.")
            df = normalize_columns(df)
            df_cleaned = clean_and_validate(df, filename, mapping)
            if not df_cleaned.empty: all_data.append(df_cleaned)
        except Exception as e: print(f"[ERRO] {e}")

    if all_data:
        print("\n>>> Consolidando dados...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # INCONSISTÊNCIAS 2: CNPJS DUPLICADOS
        print("  -> Verificando duplicidade de Razão Social por CNPJ...")
        
        # Agrupa por CNPJ e conta quantas Razões Sociais ÚNICAS existem para ele
        duplicidades = final_df.groupby('CNPJ')['RazaoSocial'].nunique()
        cnpjs_problematicos = duplicidades[duplicidades > 1]
        
        if not cnpjs_problematicos.empty:
            for cnpj in cnpjs_problematicos.index:
                nomes = final_df[final_df['CNPJ'] == cnpj]['RazaoSocial'].unique()
                logging.warning(f"GLOBAL: CNPJ {cnpj} possui múltiplas Razões Sociais diferentes: {list(nomes)}")
        
        # Filtra colunas finais
        cols_final = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'Valor Despesas']
        final_df = final_df[cols_final]
        
        final_df.to_csv(OUTPUT_FILE, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"Arquivo gerado: {OUTPUT_FILE} ({len(final_df)} linhas)")
        with zipfile.ZipFile('consolidado_despesas.zip', 'w') as zf: zf.write(OUTPUT_FILE)
    else: print("Nenhum dado gerado.")

if __name__ == "__main__":
    process_data()