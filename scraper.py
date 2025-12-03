import requests
from bs4 import BeautifulSoup
import json
import re
import datetime
import math
import urllib3

# Desabilita avisos de SSL (necessário para gov.br às vezes)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

output_file = "dados_fiscais.json"

# User-Agent "Blindado" (Chrome 122 oficial)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1'
}

def clean_float(text):
    if not text: return 0.0
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

def find_tax_table(soup, min_rows=3):
    tables = soup.find_all('table')
    print(f"   > HTML parseado. Total de tabelas encontradas: {len(tables)}")
    
    for i, table in enumerate(tables):
        rows = table.find_all('tr')
        if len(rows) < min_rows: continue
        
        parsed_data = []
        has_percent = False
        
        for row in rows:
            cols = [c.get_text(" ", strip=True) for c in row.find_all(['td', 'th'])]
            if len(cols) < 2: continue
            
            limite = None
            aliquota = None
            deducao = 0.0
            
            for txt in cols:
                txt_low = txt.lower()
                # Procura moeda (1.000,00)
                if re.search(r'\d{1,3}(?:\.\d{3})*,\d{2}', txt):
                    val = clean_float(re.search(r'\d{1,3}(?:\.\d{3})*,\d{2}', txt).group(0))
                    if 'dedu' in txt_low or 'parcela' in txt_low: deducao = val
                    elif 'até' in txt_low or 'de' in txt_low or 'salário' in txt_low: limite = val
                    elif 'acima' in txt_low: limite = 999999999.00
                
                # Procura porcentagem (7,5%)
                if '%' in txt:
                    match = re.search(r'([\d,]+)\s*%', txt)
                    if match:
                        aliquota = clean_percent(match.group(1))
                        has_percent = True
            
            if limite is not None and aliquota is not None:
                parsed_data.append({'limite': limite, 'aliquota': aliquota, 'deducao': deducao})
        
        if len(parsed_data) >= min_rows and has_percent:
            print(f"   > Tabela {i} VÁLIDA! ({len(parsed_data)} faixas)")
            return sorted(parsed_data, key=lambda x: x['limite'])
            
    return None

def fetch_inss():
    url = "https://www.gov.br/inss/pt-br/assuntos/contribuicao/tabela-de-contribuicao"
    print(f"--- Buscando INSS ({url}) ---")
    try:
        # verify=False é crucial para gov.br em ambientes serverless
        r = requests.get(url, headers=HEADERS, timeout=40, verify=False)
        print(f"   > Status Code: {r.status_code}")
        
        if r.status_code != 200:
            print(f"   > ERRO: Servidor rejeitou a conexão. Conteúdo: {r.text[:100]}")
            return None
            
        soup = BeautifulSoup(r.content, 'html.parser')
        data = find_tax_table(soup)
        if data:
            for item in data: del item['deducao']
        return data
    except Exception as e:
        print(f"   > EXCEÇÃO CRÍTICA: {e}")
        return None

def fetch_irrf():
    url = "https://www.gov.br/receitafederal/pt-br/assuntos/tributos/contribuicoes/irpf/tabelas-do-irrf"
    print(f"--- Buscando IRRF ({url}) ---")
    try:
        r = requests.get(url, headers=HEADERS, timeout=40, verify=False)
        print(f"   > Status Code: {r.status_code}")
        
        if r.status_code != 200: return None
        
        soup = BeautifulSoup(r.content, 'html.parser')
        tabela = find_tax_table(soup, min_rows=4)
        
        simplificado = 564.80
        match = re.search(r'simplificado.*?R\$\s*([\d\.,]+)', soup.get_text(), re.IGNORECASE)
        if match:
            simplificado = clean_float(match.group(1))
            print(f"   > Simplificado detectado: {simplificado}")
            
        return {'tabela': tabela, 'simplificado': simplificado} if tabela else None
    except Exception as e:
        print(f"   > EXCEÇÃO CRÍTICA: {e}")
        return None

def fetch_bacen():
    print("--- Buscando BACEN ---")
    def get(code):
        try:
            r = requests.get(f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/1?formato=json", headers={'User-Agent':'Mozilla/5.0'}, timeout=15, verify=False)
            return float(r.json()[0]['valor'].replace(',', '.'))
        except: return None
    
    selic = get(432)
    cdi = get(12)
    cdi_aa = ((1 + cdi/100)**252 - 1)*100 if cdi else None
    print(f"   > Selic: {selic} | CDI: {cdi_aa}")
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
    print("JSON finalizado.")
