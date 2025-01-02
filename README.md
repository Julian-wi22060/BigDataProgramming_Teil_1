# BigDataProgramming Teil 1 (3706017)
## Analyse des Alkoholkonsums in Russland
### Projektstruktur
```
data/
  └── alcohol_consumption_russia_17_23.csv

output/
  └── results.txt

utils/
  └── dataprocessing.py

main.py
README.md
requirements.txt
```

## Ausführung des Codes
1) In einer venv die Abhängigkeiten der requirements.txt installieren
2) Die `main.py` ausführen

---
**DISCLAIMER:**

Die Daten stammen von Kaggle: https://www.kaggle.com/datasets/scibearia/consumption-of-alcohol-in-russia-2017-2023

Allerdings muss beachtet werden, dass die Daten ursprünglich vom 'Föderalen Dienst zur Regulierung des Alkoholmarktes'
Russlands stammen und dementsprechend nicht zwingend den vollen Wahrheitsgehalt darstellen müssen.

Des Weiteren muss beachtet werden, dass die Daten nur jährliche Angaben beinhalten und entsprechend wurde das Jahr 2022 mit
dem Einmarsch Russlands in die Ukraine, welcher tatsächlich am 24. Februar 2022 stattgefunden hat, als Repräsentation des Kriegsbeginns gewählt.
Annähernd zwei Monate werden entsprechend zur Kriegszeit addiert. Die Zeit des Russisch-Ukrainischen Konflikts zuvor wird in dieser Arbeit nicht als Kriegszeit gezählt.

---

## Dokumentation

Dieses Skript analysiert Daten zum Alkoholkonsum in Russland und liefert statistische Einblicke in den Verbrauch vor und
nach Beginn des Russisch-Ukrainischen Krieges. Die Analyse umfasst:

**Datenaufbereitung:**
- Das Skript liest die Daten aus einer CSV-Datei ein
- Es wird eine neue Spalte für den Gesamtkonsum von reinem Alkohol erstellt (Liter pro Kopf)
  - Diese Berechnung basiert auf den allgemein vorherrschenden Definitionen von Alkoholgehalten der Getränkearten

**Berechnungen:**
- Durchschnittlicher reiner Alkoholkonsum pro Kopf vor und nach Beginn des Krieges.
- Durchschnittlicher Konsum der im Datensatz vorherrschenden Getränkearten in den Zeiträumen 2017–2021 (vor dem Krieg) und 2022–2023
(nach Beginn des Krieges)
- Jährliche Durchschnittswerte des reinen Alkoholkonsums
- Prozentuale Veränderungen des Konsums von Jahr zu Jahr (2017 - 2023)

**Ausgabe:**
- Ergebnisse werden sowohl in der Konsole angezeigt als auch in einer Textdatei gespeichert (`Output/results.txt`)

**Berechnung des reinen Alkoholkonsums:**
- Der reine Alkoholgehalt der verschiedenen Getränke wird wie folgt angenommen:
  - **Bier:** 5 %
  - **Wein:** 12 %
  - **Wodka:** 40 %
  - **Schaumwein:** 12 %
  - **Brandy:** 40 %
  - **Cider:** 5 %
  - **Liköre:** 20 %
- Der Gesamtkonsum reinen Alkohols wird durch die Summe dieser angenommenen Werte für jede Zeile ermittelt

---

### Zusammenfassung des reinen Alkoholkonsums

- **Durchschnittlicher reiner Alkoholkonsum pro Kopf vor dem Krieg:** 5,42 Liter
- **Durchschnittlicher reiner Alkoholkonsum pro Kopf nach Beginn des Krieges:** 5,88 Liter
  - **Steigerung:** 8,40 %

---

### Durchschnittlicher Verbrauch nach Getränk (in Litern)

| Getränk            | Vor dem Krieg (2017–2021) | Nach Beginn des Krieges (2022–2023) | Änderung (%) |
|--------------------|---------------------------|-------------------------------------|--------------|
| **Wine**           | 3.28                      | 3.51                                | +7.01        |
| **Beer**           | 45.89                     | 47.90                               | +4.38        |
| **Vodka**          | 5.44                      | 5.84                                | +7.35        |
| **Sparkling Wine** | 1.09                      | 1.29                                | +18.35       |
| **Brandy**         | 0.75                      | 0.88                                | +17.33       |
| **Сider**          | 0.51                      | 0.56                                | +9.80        |
| **Liqueurs**       | 0.49                      | 0.94                                | +91.84       |

---

### Jährlicher durchschnittlicher reiner Alkoholkonsum (in Litern)

| Jahr     | Reiner Alkoholkonsum (Liter) | Veränderung zum Vorjahr (%) |
|----------|------------------------------|-----------------------------|
| **2017** | 5.20                         | -                           |
| **2018** | 5.37                         | +3.39                       |
| **2019** | 5.36                         | -0.33                       |
| **2020** | 5.58                         | +4.09                       |
| **2021** | 5.60                         | +0.36                       |
| **2022** | 5.76                         | +2.95                       |
| **2023** | 5.99                         | +3.97                       |

---

### Interpretation der Ergebnisse

1. **Allgemeiner Anstieg des reinen Alkoholkonsums:**
   - Der durchschnittliche reine Alkoholkonsum pro Kopf stieg nach Beginn des Krieges um **8,49 %**.
   - Dieser Anstieg deutet auf verändertes Konsumverhalten während der Kriegszeit hin.

2. **Höchste Zunahmen nach Getränk:**
   - **Liköre** zeigen die größte Steigerung **(+91,84 %)**, was auf eine veränderte Präferenz oder Verfügbarkeit hindeuten könnte.
   - Andere Kategorien wie **Schaumwein (+18,35 %)** und **Brandy (+17,33 %)** weisen ebenfalls erhebliche Zunahmen auf.

3. **Jährliche Trends:**
   - Der reine Alkoholkonsum pro Kopf stieg kontinuierlich von 2020 bis 2023, mit einer deutlichen Zunahme zwischen 2022 und 2023 (+3,97 %).
   - Zwischen 2018 und 2019 gab es einen leichten Rückgang (-0,33 %).

---

### Fazit

Die Ergebnisse zeigen eine klare Tendenz zu einem Anstieg des Alkoholkonsums während und nach Beginn des Krieges,
insbesondere in Kategorien wie Likör, Schaumwein und Brandy. Eine detailliertere Analyse wäre erforderlich,
um die genauen Ursachen und Auswirkungen dieses Verhaltenswandels zu verstehen.

Der beobachtete Anstieg des Alkoholkonsums in Russland seit Beginn des Krieges könnte allerdings auf eine Kombination aus
erhöhtem Stress, wirtschaftlichen Unsicherheiten und möglicherweise veränderter Verfügbarkeit von Alkohol zurückzuführen
sein. Obwohl die im Krieg dienenden Soldaten nur einen kleinen Teil der Gesamtbevölkerung ausmachen, können die
indirekten Auswirkungen des Krieges das Konsumverhalten der gesamten Gesellschaft beeinflussen.
