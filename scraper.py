import requests
from bs4 import BeautifulSoup
import json
import re
import datetime
import math
from fake_useragent import UserAgent

output_file = "dados_fiscais.json"
ua = UserAgent()

def get_headers():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.google.com.br/'
    }

def clean_float(text):
    if not text: return 0.0
    # Troca vírgula por ponto e remove tudo que não for número
    text = text.replace('.', '').replace(',', '.')
    clean = re.sub(r'[^\d\.]', '', text)
    try: return float(clean)
    except: return 0.0

def clean_percent(text):
    if not text: return 0.0
    text = text.replace(',', '.')
    clean = re.sub(r'[^\d\.]', '', text)
    try: return float(clean) / 100.0
    except: return 0.0

# --- Lógica Genérica para achar Tabelas Fiscais ---
def find_tax_table(soup, min_rows=3):
    tables = soup.find_all('table')
    print(f"   > Encontradas {len(tables)} tabelas na página.")
    
    for i, table in enumerate(tables):
        rows = table.find_all('tr')
        # Pula tabelas muito pequenas (menus, layout)
        if len(rows) < min_rows: continue
        
        parsed_data = []
        has_percent = False
        has_currency = False
        
        for row in rows:
            cols = [c.get_text(" ", strip=True) for c in row.find_all(['td', 'th'])]
            if len(cols) < 2: continue
            
            # Tenta extrair dados da linha
            limite = None
            aliquota = None
            deducao = 0.0
            
            # Varre colunas procurando padrões
            for txt in cols:
                txt_lower = txt.lower()
                
                # Procura valores monetários (ex: 1.412,00)
                if re.search(r'\d{1,3}(?:\.\d{3})*,\d{2}', txt):
                    val = clean_float(re.search(r'\d{1,3}(?:\.\d{3})*,\d{2}', txt).group(0))
                    # Lógica para identificar se é TETO ou DEDUÇÃO
                    # Geralmente teto vem na col 0 e dedução na col 2, mas vamos tentar ser flexíveis
                    if 'dedu' in txt_lower or 'parcela' in txt_lower:
                        deducao = val
                        has_currency = True
                    elif 'até' in txt_lower or 'de' in txt_lower or 'salário' in txt_lower or 'base' in txt_lower:
                        limite = val
                        has_currency = True
                    elif 'acima' in txt_lower: # Teto infinito
                        limite = 999999999.00
                        has_currency = True
                
                # Procura porcentagem (ex: 7,5%)
                if '%' in txt:
                    match = re.search(r'([\d,]+)\s*%', txt)
                    if match:
                        aliquota = clean_percent(match.group(1))
                        has_percent = True
            
            # Se achou pelo menos um limite e uma alíquota nesta linha, guarda
            if limite is not None and aliquota is not None:
                parsed_data.append({'limite': limite, 'aliquota': aliquota, 'deducao': deducao})
        
        # Se a tabela tem dados fiscais válidos, retorna ela
        if len(parsed_data) >= min_rows and has_percent:
            print(f"   > Tabela {i} parece válida! {len(parsed_data)} faixas.")
            return sorted(parsed_data, key=lambda x: x['limite'])
            
    print("   > Nenhuma tabela válida encontrada.")
    return None

def fetch_inss():
    print("--- Buscando INSS ---")
    try:
        r = requests.get("https://www.gov.br/inss/pt-br/assuntos/contribuicao/tabela-de-contribuicao", headers=get_headers(), timeout=30)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.content, 'html.parser')
        data = find_tax_table(soup)
        # Limpa campo dedução para INSS (não usa)
        if data:
            for item in data: del item['deducao']
        return data
    except Exception as e:
        print(f"Erro: {e}")
        return None

def fetch_irrf():
    print("--- Buscando IRRF ---")
    try:
        r = requests.get("https://www.gov.br/receitafederal/pt-br/assuntos/tributos/contribuicoes/irpf/tabelas-do-irrf", headers=get_headers(), timeout=30)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # Busca tabela
        tabela = find_tax_table(soup, min_rows=4)
        
        # Busca simplificado no texto corrido
        simplificado = 564.80
        match = re.search(r'simplificado.*?R\$\s*([\d\.,]+)', soup.get_text(), re.IGNORECASE)
        if match:
            simplificado = clean_float(match.group(1))
            print(f"   > Simplificado encontrado: {simplificado}")
            
        return {'tabela': tabela, 'simplificado': simplificado} if tabela else None
    except Exception as e:
        print(f"Erro: {e}")
        return None

def fetch_bacen():
    print("--- Buscando BACEN ---")
    headers = {'User-Agent': 'Mozilla/5.0'}
    def get(code):
        try:
            r = requests.get(f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/1?formato=json", headers=headers, timeout=10)
            return float(r.json()[0]['valor'].replace(',', '.'))
        except: return None
    
    selic = get(432)
    cdi_d = get(12)
    cdi_aa = ((1 + cdi_d/100)**252 - 1)*100 if cdi_d else None
    return {'selic': selic or 15.00, 'cdi': round(cdi_aa, 2) if cdi_aa else 14.90}

if __name__ == "__main__":
    inss = fetch_inss()
    irrf = fetch_irrf()
    taxas = fetch_bacen()

    data = {
        "meta": {"atualizado": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")},
        "ano": datetime.datetime.now().year,
        "dep": 189.59,
        "inss": inss if inss else [],
        "irrf": {
            "tabela": irrf['tabela'] if irrf else [],
            "simplificado": irrf['simplificado'] if irrf else 564.80
        },
        "taxas": taxas
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
