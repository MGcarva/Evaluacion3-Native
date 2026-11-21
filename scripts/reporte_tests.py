"""Resume los reportes de surefire (app/target/surefire-reports) en texto legible para evidencias.
Uso: python scripts/reporte_tests.py [carpeta_reportes]"""
import sys, glob, os, xml.etree.ElementTree as ET

carpeta = sys.argv[1] if len(sys.argv) > 1 else os.path.join("app", "target", "surefire-reports")
tot = fal = 0
for f in sorted(glob.glob(os.path.join(carpeta, "TEST-*.xml"))):
    s = ET.parse(f).getroot()
    casos = s.findall("testcase")
    if not casos:
        continue
    print(f"## {s.get('name')}  (tests={s.get('tests')}, fallas={s.get('failures')}, errores={s.get('errors')})")
    for c in casos:
        ok = c.find("failure") is None and c.find("error") is None
        tot += 1
        fal += (not ok)
        print(f"  [{'OK ' if ok else 'FALLA'}] {c.get('name')}")
    print()
print(f"TOTAL: {tot} pruebas, {fal} fallas")
