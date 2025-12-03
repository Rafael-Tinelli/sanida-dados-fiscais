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
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
    }

def clean_float(text):
    if not text: return 0.0
    clean = text.replace('R$', '').replace('.', '').replace(',', '.').strip()
    clean = re.sub(r'[^\d\.]', '', clean)
    try: return float(clean)
    except: return 0.0

def clean_percent(text):
    if not text: return 0.0
    clean = text.replace('%', '').replace(',', '.').strip()
    try: return float(clean) / 100.0
    except: return 0.0

# 1. INSS
def fetch_inss():
    print("Buscando INSS...")
    try:
        r = requests.get("https://www.gov.br/inss/pt-br/assuntos/contribuicao/tabela-de-contribuicao", headers=get_headers(), timeout=30)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.content, 'html.parser')
        faixas = []
        for table in soup.find_all('table'):
            temp = []
            for row in table.find_all('tr'):
                cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
                if len(cols) < 2: continue
                limite, aliquota = None, None
                for txt in cols:
                    if 'até' in txt.lower():
                        nums = re.findall(r'\d{1,3}(?:\.\d{3})*,\d{2}', txt)
                        if nums: limite = clean_float(nums[-1])
                    if '%' in txt: aliquota = clean_percent(txt)
                if limite and aliquota is not None: temp.append({'limite': limite, 'aliquota': aliquota})
            if len(temp) >= 3:
                faixas = sorted(temp, key=lambda x: x['limite'])
                break
        return faixas
    except: return None

# 2. IRRF
def fetch_irrf():
    print("Buscando IRRF...")
    try:
        r = requests.get("https://www.gov.br/receitafederal/pt-br/assuntos/tributos/contribuicoes/irpf/tabelas-do-irrf", headers=get_headers(), timeout=30)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.content, 'html.parser')
        tabela, simplificado = [], 564.80
        
        match = re.search(r'simplificado.*?R\$\s*([\d\.,]+)', soup.get_text(), re.I)
        if match: simplificado = clean_float(match.group(1))

        for table in soup.find_all('table'):
            temp = []
            for row in table.find_all('tr'):
                cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
                if len(cols) < 3: continue
                base = 999999999.00 if 'acima' in cols[0].lower() else 0.0
                if base == 0.0:
                    nums = re.findall(r'\d{1,3}(?:\.\d{3})*,\d{2}', cols[0])
                    if nums: base = clean_float(nums[-1])
                ali = clean_percent(cols[1]) if '%' in cols[1] else 0.0
                ded = clean_float(cols[2])
                if base > 0: temp.append({'limite': base, 'aliquota': ali, 'deducao': ded})
            if len(temp) >= 4:
                tabela = sorted(temp, key=lambda x: x['limite'])
                break
        return {'tabela': tabela, 'simplificado': simplificado}
    except: return None

# 3. BACEN
def fetch_bacen():
    print("Buscando BACEN...")
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
    data = {
        "meta": {"atualizado": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")},
        "ano": datetime.datetime.now().year,
        "dep": 189.59,
        "inss": fetch_inss() or [],
        "irrf": fetch_irrf() or {'tabela': [], 'simplificado': 564.80},
        "taxas": fetch_bacen()
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("JSON gerado com sucesso!")
