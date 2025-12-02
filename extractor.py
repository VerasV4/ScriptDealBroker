from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

def extrair_dados_leilao(browser):
    """
    Recebe a instância do browser e retorna uma lista de dicionários
    com os dados de todos os cards de leilão visíveis na tela.
    """
    
    # Encontra todos os elementos que são cards de leilão
    # O seletor data-testid é o mais robusto disponível
    cards = browser.find_elements(By.CSS_SELECTOR, 'div[data-testid="auction-card"]')
    
    extracted_data = []
    
    print(f"Encontrados {len(cards)} cards de leilão na tela.")

    for i, card in enumerate(cards):
        try:
            # --- HELPER INTERNO ---
            # Busca o valor baseado no texto do rótulo (ex: "Faturamento")
            def get_value_by_label(label_text):
                try:
                    # XPath: Procura um SPAN com o texto exato e pega o SPAN irmão seguinte
                    elem = card.find_element(By.XPATH, f".//span[text()='{label_text}']/following-sibling::span")
                    return elem.text.strip()
                except NoSuchElementException:
                    return "N/A"

            # 1. Nome do Lead (está no atributo title do parágrafo)
            try:
                nome = card.find_element(By.CSS_SELECTOR, "p[title]").get_attribute("title")
            except:
                nome = "Desconhecido"
            
            # 2. Tipo (Lead ou Deal) 
            # Lógica: Busca a div arredondada no topo e pega o texto dentro
            try:
                tipo = card.find_element(By.XPATH, ".//div[contains(@class, 'bg-violet-500')]//span[contains(@class, 'text-neutral-50')]").text
            except:
                # Fallback genérico caso a cor mude (ex: Deal pode ter outra cor)
                try:
                    tipo = card.find_element(By.XPATH, ".//div[contains(@class, 'rounded-md')]//span[contains(@class, 'text-xs')]").text
                except:
                    tipo = "Indefinido"

            # 3. Dados Específicos usando a função helper
            faturamento = get_value_by_label("Faturamento")
            segmento = get_value_by_label("Segmento")
            produto = get_value_by_label("Tipo de produto")
            canal = get_value_by_label("Canal")

            # 4. Preço (Lógica: buscar container do rodapé esquerdo - rounded-bl-xl)
            try:
                preco_container = card.find_element(By.XPATH, ".//div[contains(@class, 'rounded-bl-xl')]")
                # Remove quebras de linha para ficar "R$ 1.000,00"
                preco = preco_container.text.replace("\n", " ").strip() 
            except:
                preco = "R$ 0,00"

            # 5. Tempo Restante (Lógica: buscar container do rodapé direito - rounded-br-xl)
            try:
                timer_container = card.find_element(By.XPATH, ".//div[contains(@class, 'rounded-br-xl')]//span[contains(@class, 'tabular-nums')]")
                tempo = timer_container.text.strip()
            except:
                tempo = "00:00:00"

            # Monta o objeto final
            dados = {
                "nome": nome,
                "tipo": tipo,
                "faturamento": faturamento,
                "segmento": segmento,
                "produto": produto,
                "canal": canal,
                "preco": preco,
                "tempo_restante": tempo
            }
            
            extracted_data.append(dados)

        except Exception as e:
            print(f"Erro ao extrair card {i+1}: {e}")
            continue

    return extracted_data