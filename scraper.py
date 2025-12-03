import requests
from bs4 import BeautifulSoup
import json
import re
import datetime
import math
from fake_useragent import UserAgent

# Configuração
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
    # Limpa R$, espaços e converte 1.000,00 para 1000.00
    clean = text.replace('R$', '').replace('.', '').replace(',', '.').strip()
    # Remove tudo que não for dígito ou ponto
    clean = re.sub(r'[^\d\.]', '', clean)
    try:
        return float(clean)
    except ValueError:
        return 0.0

def clean_percent(text):
    if not text: return 0.0
    clean = text.replace('%', '').replace(',', '.').strip()
    try:
        return float(clean) / 100.0
    except ValueError:
        return 0.0

# --- 1. INSS (gov.br) ---
def fetch_inss():
    url = "https://www.gov.br/inss/pt-br/assuntos/contribuicao/tabela-de-contribuicao"
    print(f"Buscando INSS...")
    try:
        r = requests.get(url, headers=get_headers(), timeout=30)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.content, 'html.parser')
        
        faixas = []
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            temp_faixas = []
            for row in rows:
                cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
                if len(cols) < 2: continue
                
                limite = None
                aliquota = None
                
                # Heurística para achar colunas
                for txt in cols:
                    if 'até' in txt.lower() or 'de' in txt.lower():
                        nums = re.findall(r'\d{1,3}(?:\.\d{3})*,\d{2}', txt)
                        if nums: limite = clean_float(nums[-1])
                    if '%' in txt:
                        aliquota = clean_percent(txt)
                
                if limite and aliquota is not None:
                    temp_faixas.append({'limite': limite, 'aliquota': aliquota})
            
            if len(temp_faixas) >= 3:
                faixas = sorted(temp_faixas, key=lambda x: x['limite'])
                break 
        
        return faixas if faixas else None
    except Exception as e:
        print(f"Erro INSS: {e}")
        return None

# --- 2. IRRF (Receita Federal) ---
def fetch_irrf():
    url = "https://www.gov.br/receitafederal/pt-br/assuntos/tributos/contribuicoes/irpf/tabelas-do-irrf"
    print(f"Buscando IRRF...")
    try:
        r = requests.get(url, headers=get_headers(), timeout=30)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.content, 'html.parser')
        
        tabela = []
        simplificado = 564.80 # Fallback inicial
        
        # Tenta achar valor simplificado no texto da página
        txt_pag = soup.get_text()
        match_simp = re.search(r'simplificado.*?R\$\s*([\d\.,]+)', txt_pag, re.IGNORECASE)
        if match_simp: simplificado = clean_float(match_simp.group(1))

        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            temp_tab = []
            for row in rows:
                cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
                if len(cols) < 3: continue
                
                base = 0.0
                # Detecta "Acima de" para o teto infinito
                if 'acima' in cols[0].lower(): base = 999999999.00
                else: 
                    nums = re.findall(r'\d{1,3}(?:\.\d{3})*,\d{2}', cols[0])
                    if nums: base = clean_float(nums[-1])
                
                ali = clean_percent(cols[1]) if '%' in cols[1] else 0.0
                ded = clean_float(cols[2])
                
                if base > 0:
                    temp_tab.append({'limite': base, 'aliquota': ali, 'deducao': ded})
            
            if len(temp_tab) >= 4:
                tabela = sorted(temp_tab, key=lambda x: x['limite'])
                break
        
        return {'tabela': tabela, 'simplificado': simplificado} if tabela else None
    except Exception as e:
        print(f"Erro IRRF: {e}")
        return None

# --- 3. BACEN (Selic/CDI) ---
def fetch_bacen():
    print("Buscando BACEN...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    def get_api(code):
        try:
            u = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/1?formato=json"
            r = requests.get(u, headers=headers, timeout=15)
            if r.status_code == 200:
                v = r.json()[0]['valor']
                return float(v.replace(',', '.'))
        except: return None
        return None

    selic = get_api(432) # Meta Selic
    cdi_dia = get_api(12) # CDI Diário
    
    cdi_ano = None
    if cdi_dia:
        # CDI Anualizado = (1 + taxa_dia/100)^252 - 1
        cdi_ano = (math.pow(1 + cdi_dia/100, 252) - 1) * 100
    
    return {
        'selic': selic if selic else 15.00,
        'cdi': round(cdi_ano, 2) if cdi_ano else 14.90
    }

# --- EXECUÇÃO ---
if __name__ == "__main__":
    inss = fetch_inss()
    irrf = fetch_irrf()
    taxas = fetch_bacen()

    # Monta o JSON final
    payload = {
        "meta": {
            "atualizado_em": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "autor": "GitHub Actions"
        },
        "ano": datetime.datetime.now().year,
        "dep": 189.59, # Dedução por dependente (fixo por lei há anos)
        # Se falhar o scraping, envia lista vazia e o PHP usa o fallback
        "inss": inss if inss else [], 
        "irrf": {
            "tabela": irrf['tabela'] if irrf else [],
            "simplificado": irrf['simplificado'] if irrf else 564.80
        },
        "taxas": taxas
    }

    # Salva no arquivo
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    
    print("Sucesso! Arquivo dados_fiscais.json gerado.")