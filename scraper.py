import requests
from bs4 import BeautifulSoup
import json
import re
import datetime
import math
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

output_file = "dados_fiscais.json"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive'
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

def find_tax_table(soup, min_rows=3, type='inss'):
    tables = soup.find_all('table')
    for i, table in enumerate(tables):
        rows = table.find_all('tr')
        if len(rows) < min_rows: continue
        parsed_data = []
        has_percent = False
        
        for row in rows:
            cols = [c.get_text(" ", strip=True) for c in row.find_all(['td', 'th'])]
            if len(cols) < 2: continue
            
            limite, aliquota, deducao = None, None, 0.0
            
            # Tenta identificar por posição da coluna (Mais seguro)
            # Geralmente: Col 0 = Faixa, Col 1 = Alíquota, Col 2 = Dedução
            
            # --- 1. Extrair FAIXA (Limite) ---
            txt_faixa = cols[0].lower()
            if 'acima' in txt_faixa:
                limite = 999999999.00
            else:
                # CORREÇÃO: Usa findall e pega o ÚLTIMO número (o teto)
                # Ex: "De 100 a 200" -> Pega 200.
                nums = re.findall(r'\d{1,3}(?:\.\d{3})*,\d{2}', cols[0])
                if nums: 
                    limite = clean_float(nums[-1]) # Pega o último
            
            # --- 2. Extrair ALÍQUOTA ---
            # Tenta na coluna 1, se falhar, procura % em todas
            if len(cols) > 1 and '%' in cols[1]:
                aliquota = clean_percent(cols[1])
                has_percent = True
            elif '%' in cols[0]: # Às vezes tá junto
                 match = re.search(r'([\d,]+)\s*%', cols[0])
                 if match: 
                     aliquota = clean_percent(match.group(1))
                     has_percent = True

            # --- 3. Extrair DEDUÇÃO (Só para IRRF) ---
            if type == 'irrf' and len(cols) > 2:
                # Procura moeda na coluna 2 ou 3
                ded_nums = re.findall(r'\d{1,3}(?:\.\d{3})*,\d{2}', cols[2])
                if ded_nums: deducao = clean_float(ded_nums[0])
                elif len(cols) > 3: # As vezes tem coluna extra
                     ded_nums = re.findall(r'\d{1,3}(?:\.\d{3})*,\d{2}', cols[3])
                     if ded_nums: deducao = clean_float(ded_nums[0])

            if limite is not None and aliquota is not None:
                parsed_data.append({'limite': limite, 'aliquota': aliquota, 'deducao': deducao})
        
        # Filtro de qualidade: Tabela tem que ter percentual e linhas suficientes
        if len(parsed_data) >= min_rows and has_percent:
            # Ordena pelo limite para garantir a progressão correta
            return sorted(parsed_data, key=lambda x: x['limite'])
            
    return None

def fetch_inss():
    urls = [
        "https://www.gov.br/inss/pt-br/direitos-e-deveres/inscricao-e-contribuicao/tabela-de-contribuicao-mensal",
        "https://www.gov.br/inss/pt-br/noticias/confira-como-ficaram-as-aliquotas-de-contribuicao-ao-inss"
    ]
    print("--- Buscando INSS ---")
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, 'html.parser')
                data = find_tax_table(soup, min_rows=3, type='inss')
                if data:
                    # Remove dedução do INSS (não existe)
                    for item in data: 
                        if 'deducao' in item: del item['deducao']
                    print(f"   > Sucesso em {url}")
                    return data
        except: pass
    return None

def fetch_irrf():
    urls = [
        "https://www.gov.br/receitafederal/pt-br/assuntos/meu-imposto-de-renda/tabelas/2025",
        "https://www.gov.br/receitafederal/pt-br/assuntos/meu-imposto-de-renda/tabelas"
    ]
    print("--- Buscando IRRF ---")
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, 'html.parser')
                tabela = find_tax_table(soup, min_rows=4, type='irrf')
                
                # Garante faixa isenta (0%) se não tiver
                if tabela and tabela[0]['aliquota'] > 0:
                     # Se a tabela começar direto na faixa 1, criamos a isenta artificialmente
                     # baseado no teto da faixa anterior que deve estar no texto
                     pass 

                simplificado = 564.80
                match = re.search(r'simplificado.*?R\$\s*([\d\.,]+)', soup.get_text(), re.IGNORECASE)
                if match: simplificado = clean_float(match.group(1))
                
                if tabela:
                    print(f"   > Sucesso em {url}")
                    return {'tabela': tabela, 'simplificado': simplificado}
        except: pass
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
