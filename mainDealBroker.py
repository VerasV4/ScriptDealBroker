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
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import traceback
import logging

# IMPORTA A LÓGICA DE EXTRAÇÃO DO OUTRO ARQUIVO
from extractor import extrair_dados_leilao

# ──────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURAÇÕES GERAIS
# ──────────────────────────────────────────────────────────────────────────────
SPREADSHEET_ID   = "1zGQjh1s50gQBlEoEFJiRs5ERfYYqRxSI_YxN4asgQU8"
TARGET_RANGE     = "dealBroker!A2:I"
CREDENTIALS_FILE = "docs-api-call-d7e6aa8d3712.json"
TARGET_URL       = "https://brokers.mktlab.app/signin"
LOGIN            = 'jp.azevedo@v4company.com'
PASSWORD         = 'Peedriinho459!'

# Cache Global para evitar leituras repetidas na API
# Armazena os nomes/IDs dos leads já processados nesta sessão
PROCESSED_LEADS_CACHE = set()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ──────────────────────────────────────────────────────────────────────────────
# 2. FUNÇÕES AUXILIARES
# ──────────────────────────────────────────────────────────────────────────────
def get_priority_score(faturamento):
    score = 0
    if "400 mil" in faturamento or "milhões" in faturamento:
        score += 30
    elif "101 mil" in faturamento:
        score += 10
    return score

def send_lead_to_whatsapp(lead_data):
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
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MzczNzI4MzksImlzcyI6InphcHN0ZXJhcGkiLCJzdWIiOiI9NjgyZjg2NC1jMDc5LTQ0YjMtYjhkMC1lOWEzZGJjZTU2MTgiLCJqdGkiOiI3ZjlkMjg2MS04MjdmLTQ2NDQtOGIyZS1kMWQyZTdkNjM3MWQifQ.XBo9IITzeVidiPEV6VPuMdBI-bj7jZ-c_BkKnEGwoLI",
        "Content-Type": "application/json"
    }

    try:
        requests.request("POST", url, json=payload, headers=headers)
    except Exception as e:
        print(f"Erro ao enviar WhatsApp: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# 3. FUNÇÕES DO GOOGLE SHEETS
# ──────────────────────────────────────────────────────────────────────────────
def load_initial_cache(credentials_file, spreadsheet_id, range_name):
    """
    ### CORREÇÃO:
    Carrega os dados da planilha APENAS UMA VEZ no início para popular o cache.
    """
    print("Carregando histórico da planilha para memória...")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name
        ).execute()
        rows = result.get('values', [])
        
        # Pega a coluna 1 (Nome/ID) de cada linha, remove espaços e adiciona ao SET global
        for row in rows:
            if len(row) > 1:
                # Normaliza para string e remove espaços extras
                PROCESSED_LEADS_CACHE.add(str(row[1]).strip())
                
        print(f"Cache inicializado com {len(PROCESSED_LEADS_CACHE)} leads já existentes.")
        
    except Exception as e:
        print(f"ERRO CRÍTICO ao ler planilha inicial: {e}")
        # Se der erro na leitura inicial, é melhor parar do que duplicar tudo
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
        print(f"Planilha atualizada: {len(cards_data)} novos registros.")
    except Exception as e:
        print(f"Erro ao gravar na planilha: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# 4. AUTOMAÇÃO E LOGIN (SELENIUM)
# ──────────────────────────────────────────────────────────────────────────────
def perform_login(browser):
    print("Navegando para página de login...")
    browser.get(TARGET_URL)

    try:
        WebDriverWait(browser, 20).until(
            EC.element_to_be_clickable((By.ID, "email"))
        ).send_keys(LOGIN)

        browser.find_element(By.ID, "password").send_keys(PASSWORD)
        browser.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        print("Credenciais enviadas.")

        WebDriverWait(browser, 30).until(
            EC.invisibility_of_element_located((By.ID, "email"))
        )
        print("Login realizado com sucesso.")
        time.sleep(5)

    except Exception as e:
        print(f"Erro no login: {e}")
        browser.save_screenshot("erro_login.png")
        raise e

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

    try:
        perform_login(browser)
    except Exception:
        browser.quit()
        raise

    return browser

# ──────────────────────────────────────────────────────────────────────────────
# 5. ORQUESTRAÇÃO
# ──────────────────────────────────────────────────────────────────────────────
def process_and_save_cards(browser, credentials_file, spreadsheet_id, range_name):
    """
    Função principal ajustada para usar CACHE local.
    """
    print("Verificando cards na tela...")

    try:
        WebDriverWait(browser, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="cards-grid"]'))
        )
    except Exception:
        if len(browser.find_elements(By.ID, "email")) > 0:
            raise Exception("SESSION_EXPIRED")
        print("Nenhum card visível no momento.")
        return

    extracted_data = extrair_dados_leilao(browser)
    
    if not extracted_data:
        return

    data_entrada = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    rows_to_insert = []

    for item in extracted_data:
        # ### CORREÇÃO: Normalização do ID (String e Strip)
        nome_lead = str(item['nome']).strip()
        
        # ### CORREÇÃO: Verifica no CACHE (memória) ao invés de ler a planilha
        if nome_lead in PROCESSED_LEADS_CACHE:
            # print(f"Lead {nome_lead} já processado. Pulando.")
            continue

        # Se chegou aqui, é novo
        print(f"NOVO LEAD ENCONTRADO: {nome_lead}")
        
        # 1. Adiciona ao cache imediatamente para não duplicar no próximo loop
        PROCESSED_LEADS_CACHE.add(nome_lead)
        
        # 2. Envia WhatsApp
        send_lead_to_whatsapp(item)
        
        # 3. Prepara linha para Planilha
        row = [
            data_entrada,
            nome_lead, # Garante que vai limpo
            item['tipo'],
            item['segmento'],
            item['faturamento'],
            item['produto'],
            item['canal'],
            item['preco'],
            item['tempo_restante']
        ]
        rows_to_insert.append(row)

    # Gravar em lote na planilha apenas os novos
    if rows_to_insert:
        append_data_to_sheets(credentials_file, spreadsheet_id, range_name, rows_to_insert)
    else:
        print("Sem novos leads ÚNICOS para gravar.")

# ──────────────────────────────────────────────────────────────────────────────
# 6. LOOP PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────
def main():
    # 1. Carrega o cache ANTES de abrir o browser
    load_initial_cache(CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE)
    
    # 2. Abre Browser
    browser = setup_browser_and_login()
    
    try:
        # Primeira verificação
        process_and_save_cards(browser, CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE)

        while True:
            print("Aguardando 2s...")
            time.sleep(2)
            
            print("Atualizando página...")
            browser.refresh()
            time.sleep(5) 
            
            try:
                process_and_save_cards(browser, CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE)
            except Exception as e:
                if "SESSION_EXPIRED" in str(e):
                    print("Sessão expirada. Tentando relogar...")
                    perform_login(browser)
                else:
                    print(f"Erro no loop: {e}")

    except KeyboardInterrupt:
        print("Parando script...")
    finally:
        browser.quit()

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"Crash Crítico: {e}. Reiniciando em 10s...")
            time.sleep(10)