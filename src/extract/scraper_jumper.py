import requests
from bs4 import BeautifulSoup
import re
import os
import csv
import logging
from datetime import datetime

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuração de caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "jumper_brasil")

def extract_jumper_to_datalake():
    url = "https://jumperbrasil.com.br/nba-2025-26-calendario-de-transmissoes-da-tv-para-o-brasil/"
    logging.info("Acessando Jumper Brasil para extrair grade de TV...")
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        padrao_data = re.compile(r'(\d{1,2}/\d{1,2}/\d{2})(?:\s*\([^)]+\))?')
        atual_data = None
        jogos_extraidos = []
        
        texto_completo = soup.get_text()
        linhas = texto_completo.split('\n')
        
        for linha in linhas:
            linha = linha.strip()
            if not linha:
                continue
            
            # Atualiza a data vigente
            match_data = padrao_data.search(linha)
            if match_data and ' x ' not in linha:
                atual_data = match_data.group(1)
                continue
            
            # Identifica linha de jogo
            if atual_data and ' x ' in linha and 'h' in linha and '(' in linha:
                try:
                    linha_limpa = re.sub(r'^\d{1,2}/\d{1,2}/\d{2}\s*\([^)]+\)\s*', '', linha)
                    
                    if '–' in linha_limpa:
                        partes = linha_limpa.split('–', 1)
                    elif ' - ' in linha_limpa:
                        partes = linha_limpa.split(' - ', 1)
                    else:
                        continue
                    
                    if len(partes) < 2:
                        continue
                    
                    times_part = partes[0].strip()
                    horario_canal_part = partes[1].strip()
                    
                    times = times_part.split(' x ')
                    if len(times) != 2:
                        continue
                    
                    visitante = times[0].strip()
                    mandante = times[1].strip()
                    
                    horario_match = re.search(r'(\d{1,2}h\d{0,2})', horario_canal_part)
                    if not horario_match:
                        continue
                    horario = horario_match.group(1)
                    
                    canal_match = re.search(r'\(([^)]+)\)', horario_canal_part)
                    if not canal_match:
                        continue
                    canais_texto = canal_match.group(1)
                    
                    if '/' in canais_texto:
                        canais = [c.strip() for c in canais_texto.split('/')]
                        canal = ' / '.join(canais)
                    else:
                        canal = canais_texto.strip()
                    
                    # Salva o dado bruto como um dicionário
                    jogos_extraidos.append({
                        "data_br": atual_data,
                        "visitante": visitante,
                        "mandante": mandante,
                        "horario_br": horario,
                        "canal_br": canal
                    })
                    
                except Exception as e:
                    logging.warning(f"Erro ao parsear linha '{linha}': {e}")
                    
        if not jogos_extraidos:
            logging.warning("Nenhum jogo extraído. O layout do site pode ter mudado.")
            return

        # Prepara a pasta Bronze do dia
        hoje_str = datetime.now().strftime('%Y-%m-%d')
        target_dir = os.path.join(BRONZE_DIR, hoje_str)
        os.makedirs(target_dir, exist_ok=True)
        
        file_path = os.path.join(target_dir, "canais_jumper_raw.csv")
        
        # Salva a lista de dicionários como CSV
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['data_br', 'visitante', 'mandante', 'horario_br', 'canal_br']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for jogo in jogos_extraidos:
                writer.writerow(jogo)
                
        logging.info(f"Extração concluída: {len(jogos_extraidos)} jogos salvos na Bronze.")
        logging.info(f"Arquivo salvo em: {file_path}")
        
    except Exception as e:
        logging.error(f"Erro na extração: {e}")

if __name__ == "__main__":
    extract_jumper_to_datalake()