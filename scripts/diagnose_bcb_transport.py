"""Read-only BCB reachability diagnostic for the GitHub-hosted runner.

Only two official SGS series are probed, with an alternate official query
shape for Selic. Does not update runtime state or any production artifact.
Diagnostics are deliberately non-gating; the source producers retain their
fail-closed gates and may not infer a valid rate from these probes.
"""
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import time
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
import requests

HEADERS={"User-Agent":"SanidaFiscalTransportProbe/1.0 (+https://sanida.com.br)","Accept":"application/json"}
def urls():
    current=datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo")).date()
    params=urlencode({
        "formato":"json","dataInicial":(current-timedelta(days=31)).strftime("%d/%m/%Y"),
        "dataFinal":current.strftime("%d/%m/%Y"),
    })
    root="https://api.bcb.gov.br/dados/serie/bcdata.sgs."
    return [
        ("selic_432_bounded",root+"432/dados?"+params),
        ("selic_432_latest",root+"432/dados/ultimos/1?formato=json"),
        ("cdi_12_latest",root+"12/dados/ultimos/1?formato=json"),
    ]
def report(name,transport,response,elapsed):
    valid=False
    if response.status_code==200:
        try:
            payload=response.json()
            valid=isinstance(payload,list) and bool(payload) and all(
                isinstance(x,dict) and "data" in x and "valor" in x
                for x in payload
            )
        except (json.JSONDecodeError,UnicodeError,ValueError):
            valid=False
    digest=sha256(response.content).hexdigest() if response.content else None
    print(json.dumps({
        "source":name,"transport":transport,"status":response.status_code,
        "sgs_json_valid":valid,"byte_count":len(response.content),
        "body_sha256":digest,"elapsed_s":round(elapsed,2),
    },sort_keys=True),flush=True)
def probe(name,url,transport):
    start=time.monotonic()
    try:
        if transport=="httpx":
            with httpx.Client(timeout=5.0,follow_redirects=True) as client:
                response=client.get(url,headers=HEADERS)
        else:
            response=requests.get(url,headers=HEADERS,timeout=5.0,allow_redirects=True)
        report(name,transport,response,time.monotonic()-start)
    except (httpx.RequestError,requests.RequestException) as exc:
        print(json.dumps({
            "source":name,"transport":transport,"error_type":type(exc).__name__,
            "elapsed_s":round(time.monotonic()-start,2),
        },sort_keys=True),flush=True)
def main():
    for name,url in urls():
        for transport in ("httpx","requests"):
            probe(name,url,transport)
    print("BCB_TRANSPORT_PROBE_FINISHED; evidence only; no artifact updated",flush=True)
if __name__=="__main__":
    main()
