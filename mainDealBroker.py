import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import os
import time
import datetime
import pytz  # BIBLIOTECA PARA FUSO HORÁRIO
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import traceback

# IMPORTA A LÓGICA DE EXTRAÇÃO
from extractor import extrair_dados_leilao

# ──────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURAÇÕES E LOGS
# ──────────────────────────────────────────────────────────────────────────────
SPREADSHEET_ID   = "1zGQjh1s50gQBlEoEFJiRs5ERfYYqRxSI_YxN4asgQU8"
TARGET_RANGE     = "dealBroker!A2:I"
CREDENTIALS_FILE = "docs-api-call-d7e6aa8d3712.json"
TARGET_URL       = "https://brokers.mktlab.app/signin"
LOGIN            = 'jp.azevedo@v4company.com'
PASSWORD         = 'Peedriinho459!'

# Configuração de Fuso Horário (Brasil)
TZ_BR = pytz.timezone('America/Sao_Paulo')

# Cache Global (Memória)
PROCESSED_LEADS_CACHE = set()

# Configuração de Logging (Salva em arquivo E mostra na tela)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("dealbroker.log"), # Salva no arquivo
        logging.StreamHandler()                # Mostra no terminal
    ]
)

# ──────────────────────────────────────────────────────────────────────────────
# 2. FUNÇÕES AUXILIARES
# ──────────────────────────────────────────────────────────────────────────────
def get_now_br():
    """Retorna data/hora atual no fuso do Brasil."""
    return datetime.datetime.now(TZ_BR)

def get_priority_score(faturamento):
    score = 0
    if "400 mil" in faturamento or "milhões" in faturamento:
        score += 30
    elif "101 mil" in faturamento:
        score += 10
    return score

def send_lead_to_whatsapp(lead_data):
    """Envia lead para WhatsApp com tratamento de erro."""
    url = "https://api.zapsterapi.com/v1/wa/messages"
    
    priority_score = get_priority_score(lead_data.get('faturamento', ''))
    priority_label = "⭐️⭐️⭐️" if priority_score >= 30 else "⭐️"
    
    message = f"""
*Novo {lead_data['tipo']} Detectado! {priority_label}*
Nome: {lead_data['nome']}
Segmento: {lead_data['segmento']}
Faturamento: {lead_data['faturamento']}
Valor Atual: {lead_data['preco']}
Tempo Restante: {lead_data['tempo_restante']}

*Detalhes:*
- Produto: {lead_data.get('produto', 'N/A')}
- Canal: {lead_data.get('canal', 'N/A')}
    """
    
    payload = {
        "recipient": "group:120363371362817488",
        "text": message,
        "instance_id": "h3t9n2wmfmne9i2c4s5s6"
    }
    
    headers = {
        # Token deve ser válido
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MzczNzI4MzksImlzcyI6InphcHN0ZXJhcGkiLCJzdWIiOiI5NjgyZjg2NC1jMDc5LTQ0YjMtYjhkMC1lOWEzZGJjZTU2MTgiLCJqdGkiOiI3ZjlkMjg2MS04MjdmLTQ2NDQtOGIyZS1kMWQyZTdkNjM3MWQifQ.XBo9IITzeVidiPEV6VPuMdBI-bj7jZ-c_BkKnEGwoLI",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
            logging.info(f"✅ WhatsApp enviado: {lead_data['nome']}")
        else:
            logging.error(f"❌ Erro WhatsApp ({response.status_code}): {response.text}")
    except Exception as e:
        logging.error(f"❌ Exceção WhatsApp: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# 3. FUNÇÕES DO GOOGLE SHEETS
# ──────────────────────────────────────────────────────────────────────────────
def load_initial_cache(credentials_file, spreadsheet_id, range_name):
    """Carrega apenas leads de HOJE para a memória."""
    logging.info("Carregando histórico do dia na planilha...")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name
        ).execute()
        rows = result.get('values', [])
        
        hoje_str = get_now_br().strftime("%d/%m/%Y")
        count = 0
        
        for row in rows:
            if len(row) > 1:
                # Data na coluna A (índice 0) ex: "03/12/2025 11:45"
                data_row = row[0].split(' ')[0] 
                nome_lead = str(row[1]).strip()
                
                if data_row == hoje_str:
                    PROCESSED_LEADS_CACHE.add(nome_lead)
                    count += 1
                
        logging.info(f"Cache inicializado: {count} leads já processados HOJE ({hoje_str}).")
        
    except Exception as e:
        logging.critical(f"Erro ao ler planilha inicial: {e}")
        raise e

def append_data_to_sheets(credentials_file, spreadsheet_id, range_name, cards_data):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)

    body = {'values': cards_data}

    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        logging.info(f"Planilha atualizada: {len(cards_data)} novos registros.")
    except Exception as e:
        logging.error(f"Erro ao gravar Sheets: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# 4. AUTOMAÇÃO E LOGIN
# ──────────────────────────────────────────────────────────────────────────────
def setup_browser_and_login():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    browser = webdriver.Chrome(service=service, options=options)

    logging.info("Navegando para login...")
    try:
        browser.get(TARGET_URL)
        WebDriverWait(browser, 20).until(EC.element_to_be_clickable((By.ID, "email"))).send_keys(LOGIN)
        browser.find_element(By.ID, "password").send_keys(PASSWORD)
        browser.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        
        WebDriverWait(browser, 30).until(EC.invisibility_of_element_located((By.ID, "email")))
        logging.info("Login realizado com sucesso.")
        
        # Salva print apenas para debug se necessário (sobrescreve o anterior)
        browser.save_screenshot("debug_login_last.png") 

    except Exception as e:
        logging.error(f"Erro no login: {e}")
        browser.save_screenshot("debug_login_error.png")
        browser.quit()
        raise e

    return browser

# ──────────────────────────────────────────────────────────────────────────────
# 5. ORQUESTRAÇÃO
# ──────────────────────────────────────────────────────────────────────────────
def process_and_save_cards(browser, credentials_file, spreadsheet_id, range_name):
    # logging.info("Verificando cards...") # Comentado para não poluir log a cada 5s

    try:
        WebDriverWait(browser, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="cards-grid"]'))
        )
    except Exception:
        if len(browser.find_elements(By.ID, "email")) > 0:
            raise Exception("SESSION_EXPIRED")
        return

    extracted_data = extrair_dados_leilao(browser)
    if not extracted_data:
        return

    data_entrada = get_now_br().strftime("%d/%m/%Y %H:%M")
    rows_to_insert = []

    for item in extracted_data:
        nome_lead = str(item['nome']).strip()
        
        # Verifica Cache
        if nome_lead in PROCESSED_LEADS_CACHE:
            continue

        logging.info(f"NOVO LEAD ENCONTRADO: {nome_lead}")
        PROCESSED_LEADS_CACHE.add(nome_lead)
        
        send_lead_to_whatsapp(item)
        
        row = [
            data_entrada,
            nome_lead,
            item['tipo'],
            item['segmento'],
            item['faturamento'],
            item['produto'],
            item['canal'],
            item['preco'],
            item['tempo_restante']
        ]
        rows_to_insert.append(row)

    if rows_to_insert:
        append_data_to_sheets(credentials_file, spreadsheet_id, range_name, rows_to_insert)

# ──────────────────────────────────────────────────────────────────────────────
# 6. LOOP PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────
def main():
    load_initial_cache(CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE)
    browser = setup_browser_and_login()
    
    try:
        process_and_save_cards(browser, CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE)

        while True:
            time.sleep(2)
            # logging.info("Refresh...") # Opcional
            browser.refresh()
            time.sleep(5)
            
            try:
                process_and_save_cards(browser, CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE)
            except Exception as e:
                if "SESSION_EXPIRED" in str(e):
                    logging.warning("Sessão expirada. Relogando...")
                    browser.quit()
                    browser = setup_browser_and_login()
                else:
                    logging.error(f"Erro no loop: {e}")

    except KeyboardInterrupt:
        logging.info("Parando script...")
    finally:
        browser.quit()

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            logging.critical(f"Crash Crítico: {e}. Reiniciando em 10s...")
            time.sleep(10)