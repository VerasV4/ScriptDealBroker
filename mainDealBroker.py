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
TARGET_RANGE     = "dealBroker!A2:I"        # Range ajustado para 9 colunas
CREDENTIALS_FILE = "docs-api-call-d7e6aa8d3712.json"
TARGET_URL       = "https://brokers.mktlab.app/signin"
LOGIN            = 'jp.azevedo@v4company.com'
PASSWORD         = 'Peedriinho459!'

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ──────────────────────────────────────────────────────────────────────────────
# 2. FUNÇÕES AUXILIARES (PONTUAÇÃO E WHATSAPP)
# ──────────────────────────────────────────────────────────────────────────────
def get_priority_score(faturamento):
    """Calcula pontuação de prioridade baseado no faturamento."""
    score = 0
    if "400 mil" in faturamento or "milhões" in faturamento:
        score += 30
    elif "101 mil" in faturamento:
        score += 10
    return score

def send_lead_to_whatsapp(lead_data):
    """Envia lead para WhatsApp via API com priorização."""
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
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MzczNzI4MzksImlzcyI6InphcHN0ZXJhcGkiLCJzdWIiOiI5NjgyZjg2NC1jMDc5LTQ0YjMtYjhkMC1lOWEzZGJjZTU2MTgiLCJqdGkiOiI3ZjlkMjg2MS04MjdmLTQ2NDQtOGIyZS1kMWQyZTdkNjM3MWQifQ.XBo9IITzeVidiPEV6VPuMdBI-bj7jZ-c_BkKnEGwoLI",
        "Content-Type": "application/json"
    }

    try:
        response = requests.request("POST", url, json=payload, headers=headers)
        # print(f"WhatsApp Status: {response.status_code}")
    except Exception as e:
        print(f"Erro ao enviar WhatsApp: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# 3. FUNÇÕES DO GOOGLE SHEETS
# ──────────────────────────────────────────────────────────────────────────────
def get_existing_data(credentials_file, spreadsheet_id, range_name):
    """Lê os dados existentes para evitar duplicatas."""
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name
        ).execute()
        return result.get('values', [])
    except Exception as e:
        print(f"Erro ao ler planilha: {e}")
        return []

def append_data_to_sheets(credentials_file, spreadsheet_id, range_name, cards_data):
    """Grava novas linhas na planilha."""
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)

    body = {'values': cards_data}

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()
    print(f"Planilha atualizada: {len(cards_data)} novos registros.")

# ──────────────────────────────────────────────────────────────────────────────
# 4. AUTOMAÇÃO E LOGIN (SELENIUM)
# ──────────────────────────────────────────────────────────────────────────────
def perform_login(browser):
    """Realiza login e tira print de confirmação."""
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
        browser.save_screenshot("login_sucesso.png")
        print("📸 Print salvo: login_sucesso.png")

    except Exception as e:
        print(f"Erro no login: {e}")
        browser.save_screenshot("erro_login.png")
        raise e

def setup_browser_and_login():
    """Configura o Chrome Options e inicia o browser."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless") # Mude para False se quiser ver a tela
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
# 5. ORQUESTRAÇÃO (PROCESSAR CARDS)
# ──────────────────────────────────────────────────────────────────────────────
def process_and_save_cards(browser, credentials_file, spreadsheet_id, range_name, first_run=False):
    """
    Função principal que chama o Extractor, verifica duplicatas e salva no Sheets.
    """
    print("Verificando cards na tela...")

    try:
        # Aguarda o carregamento do GRID de cards para garantir que o site carregou
        WebDriverWait(browser, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="cards-grid"]'))
        )
    except Exception:
        # Se não achar o grid, verifica se a sessão caiu (input de email voltou)
        if len(browser.find_elements(By.ID, "email")) > 0:
            raise Exception("SESSION_EXPIRED")
        print("Nenhum card visível no momento.")
        return

    # 1. CHAMADA AO ARQUIVO EXTERNO (extractor.py)
    extracted_data = extrair_dados_leilao(browser)
    
    if not extracted_data:
        return

    # 2. Lógica de Duplicidade (Baseada no Nome do Lead)
    existing_sheet_data = get_existing_data(credentials_file, spreadsheet_id, range_name)
    # Assumindo coluna B (índice 1) como Nome do Lead na planilha
    existing_titles = [row[1] for row in existing_sheet_data if len(row) > 1]

    data_entrada = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    rows_to_insert = []

    for item in extracted_data:
        nome_lead = item['nome']
        
        # Se não é a primeira execução e já existe na planilha, pula
        if not first_run and nome_lead in existing_titles:
            continue

        # Se for novo, prepara para enviar e salvar
        if nome_lead not in existing_titles:
            # Envia WhatsApp
            send_lead_to_whatsapp(item)
            
            # Monta a linha para o Google Sheets (Ordem das Colunas)
            # A:Data | B:Nome | C:Tipo | D:Segmento | E:Faturamento | F:Produto | G:Canal | H:Valor | I:Tempo
            row = [
                data_entrada,
                item['nome'],
                item['tipo'],
                item['segmento'],
                item['faturamento'],
                item['produto'],
                item['canal'],
                item['preco'],
                item['tempo_restante']
            ]
            rows_to_insert.append(row)

    # 3. Salvar em lote na planilha
    if rows_to_insert:
        append_data_to_sheets(credentials_file, spreadsheet_id, range_name, rows_to_insert)
    else:
        print("Sem novos leads para gravar.")

# ──────────────────────────────────────────────────────────────────────────────
# 6. LOOP PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────
def main():
    browser = setup_browser_and_login()
    
    try:
        # Primeira execução
        process_and_save_cards(browser, CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE, first_run=True)

        while True:
            print("Aguardando 30s...")
            time.sleep(30)
            
            print("Atualizando página...")
            browser.refresh()
            time.sleep(5) # Espera renderizar após refresh
            
            try:
                process_and_save_cards(browser, CREDENTIALS_FILE, SPREADSHEET_ID, TARGET_RANGE, first_run=False)
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