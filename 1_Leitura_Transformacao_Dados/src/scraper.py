import os 
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import urllib3

# Suprime os avisos de segurança por não verificar o SSL (necessário para sites gov.br)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL_BASE = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

# Retorna o objeto BeautifulSoup de uma URL.
def get_soup(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return None

# Busca todos os links (href) em uma página.
def get_links(url):
    soup = get_soup(url)
    if not soup:
        return []
    
    links = []
    for a in soup.find_all('a'):
        href = a.get('href')
        if href and not href.startswith('?') and href != "../":
            links.append(href)
            
    return links

# Navega nas pastas de anos e encontra os links diretos para os 3 últimos .zip disponíveis.
def find_last_3_quarters_files():
    print("Buscando anos disponíveis...")
    
    # 1. Pega os anos disponíveis na raiz
    root_links = get_links(URL_BASE)
    # Filtra apenas strings que são anos (4 dígitos), ex: '2024/', '2023/'
    years = sorted([y for y in root_links if re.match(r'^\d{4}/?$', y)], reverse=True)
    
    print(f"Anos encontrados: {years[:3]}") # Mostra os 3 primeiros anos
    
    zip_files_found = []
    
    # 2. Entra em cada ano (do mais recente para o mais antigo)
    for year in years:
        if len(zip_files_found) >= 3:
            break
        year_url = urljoin(URL_BASE, year)
        print(f"Verificando dentro do ano: {year}")
        
        items = get_links(year_url)
            
        # 3. Filtra arquivos .zip que parecem ser trimestres
        quarter_zips = []
        for item in items:
            # Procura por "1T2024", "1t24", "trimestre", etc.
            if item.lower().endswith('.zip') and ('t' in item.lower() or 'trimestre' in item.lower()):
                quarter_zips.append(item)
        
        # Ordena decrescente (3T > 2T > 1T)
        quarter_zips = sorted(quarter_zips, reverse=True)
        
        for q_zip in quarter_zips:
            if len(zip_files_found) >= 3:
                break
            
            full_url = urljoin(year_url, q_zip)
            print(f"  -> Encontrado: {q_zip}")
            zip_files_found.append({
                "filename": q_zip,
                "url": full_url
            })

    return zip_files_found

# Baixa os arquivos .zip identificados.
def download_files(target_dir="data/raw"):
    os.makedirs(target_dir, exist_ok=True)
    
    files_to_download = find_last_3_quarters_files()
    downloaded_paths = []

    if not files_to_download:
        print("Nenhum arquivo encontrado. Verifique a conexão ou a URL base.")
        return []

    print(f"\nArquivos selecionados para download: {[f['filename'] for f in files_to_download]}")

    for item in files_to_download:
        file_name = item['filename']
        file_url = item['url']
        local_path = os.path.join(target_dir, file_name)
        
        # Evita baixar de novo se já existe
        if os.path.exists(local_path):
            print(f"  -> {file_name} já existe. Pulando.")
            downloaded_paths.append(local_path)
            continue
            
        print(f"  -> Baixando {file_name}...", end='', flush=True)
        
        try:
            # Usa verify=False também no download
            with requests.get(file_url, stream=True, verify=False, timeout=60) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(" [OK]")
            downloaded_paths.append(local_path)
        except Exception as e:
            print(f" [ERRO]\n     Falha ao baixar {file_name}: {e}")
    
    return downloaded_paths

if __name__ == "__main__":
    download_files()