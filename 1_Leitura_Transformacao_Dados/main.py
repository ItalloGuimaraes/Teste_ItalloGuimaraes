import sys
import os

# Adiciona o diretório atual ao path para garantir que o python encontre o pacote 'src'
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.scraper import download_files
from src.processor import process_data

def main():
    print("===================================================")
    print("   INICIANDO TESTE 1: ETL DADOS ANS")
    print("===================================================")
    
    # Passo 1: Extração (Crawler)
    print("\n[1/2] Executando Scraper (Download)...")
    try:
        download_files()
    except Exception as e:
        print(f"ERRO CRÍTICO NO SCRAPER: {e}")
        return

    # Passo 2: Transformação e Carga
    print("\n[2/2] Executando Processamento (ETL)...")
    try:
        process_data()
    except Exception as e:
        print(f"ERRO CRÍTICO NO PROCESSADOR: {e}")
        return

    print("\n===================================================")
    print("   SUCESSO! Arquivo 'consolidado_despesas.zip' gerado.")
    print("===================================================")

if __name__ == "__main__":
    main()