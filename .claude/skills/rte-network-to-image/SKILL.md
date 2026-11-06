---
name: rte-network-to-image
description: Zeichnet aus einem bestehenden rte.Network (natsfbp, Go-Datei) ein SVG-Bild des FBP-Netzwerks neben der Go-Datei – der umgekehrte Weg zu rte-network-from-image. Der Name kommt immer aus dem Dateinamen der Go-Datei. Verwenden, wenn der Nutzer aus einem rte.Network/Netzwerk/Go-Code ein Bild, Diagramm oder eine SVG erzeugen oder ein bestehendes Bild an den Code anpassen will. Aufruf: /rte-network-to-image <pfad/zur/datei.go>
argument-hint: <pfad/zur/datei.go>
---

# SVG-Bild aus einem rte.Network

Argument: Pfad zu einer Go-Datei mit einem rte.Network, z. B. `pkg/components/writerandom23.go`.
Fehlt es und wurde in dieser Session genau ein Network angelegt oder geändert, dieses nehmen und das am Ende
erwähnen. Sonst den Nutzer nach dem Pfad fragen.

Gegenstück zu `rte-network-from-image`: Das erzeugte SVG muss so aufgebaut sein, dass `/rte-network-from-image` daraus wieder genau dasselbe Network erzeugen könnte (Round-Trip).

## Namen

- SVG-Datei = Dateiname der Go-Datei mit `.svg` statt `.go`, im selben Ordner, z. B. `pkg/components/writerandom23.svg`.
  Ein Suffix `_net` fällt dabei weg (Projektkonvention): `pkg/jira/createcustomer_net.go` →
  `pkg/jira/createcustomer.svg`, wie bei `addhellocomment_net.go` ↔ `addhellocomment.svg`. Sonst würde
  `/rte-network-from-image` daraus ein Network `createcustomer_net` bauen, und der Round-Trip wäre kaputt.
- Titel im Bild = Name des Go-Typs, der `rte.Network` einbettet (z. B. `Writerandom23`).
- Existiert die SVG schon, nicht überschreiben, sondern den Nutzer fragen (überschreiben, oder Unterschiede zwischen Bild und Code nur auflisten).
- Enthält die Datei kein oder mehr als ein rte.Network, abbrechen bzw. den Nutzer fragen, welches gemeint ist.

## Go-Code lesen

Aus der Network-Datei:

- **Netz-Inport:** immer `Inport` (kommt aus `rte.Network`).
- **Netz-Outports:** alle Felder `<Name> rte.Outport` im Network-Struct.
- **Bricks:** in `Init()` jede Zeile `<var> := new(<Typ>).New(...)` → Variable ↔ Komponententyp; jede `this.AddNode("<knoten>", <var>)` → Knotenname ↔ Variable.
- **Verbindungen** in `Init()`:
  - `this.ConInport(this.Inport, "<knoten>", <var>.<Inport>)` → Netz-Inport → Brick-Inport
  - `this.Con("<von>", <var>.<Outport>, "<nach>", <var>.<Inport>)` → Brick-Outport → Brick-Inport
  - `this.ConOutport(this.<Outport>, "<knoten>", <var>.<Outport>)` → Brick-Outport → Netz-Outport
- **Beschreibung:** der Kommentar über dem Typ (nur zur Kontrolle, kommt nicht ins Bild).

Für jeden Komponententyp die Go-Datei im Paket öffnen und die Ports aus dem Struct lesen:

- Inport: `Inport` (aus `rte.Component` bzw. `rte.Network`).
- Outports: alle eigenen Felder vom Typ `rte.Outport`. Ist die Komponente selbst ein rte.Network, sind das ihre Netz-Outports.
- `Fail` und `Error` aus `rte.Component` **nicht** zeichnen (stehen nur als Hinweis in der Legende).

Alle Outports, die nicht verbunden sind, und Inports ohne eingehende Verbindung bekommen den „offen“-Stummel. Gibt es einen Komponententyp oder ein Port-Feld nicht, abbrechen und den Nutzer fragen, statt etwas zu erfinden.

## Layout

Fluss von links nach rechts:

- **Spalten:** Bricks nach Tiefe im Graphen anordnen. Bricks, die am Netz-Inport hängen, = Spalte 0; jeder andere Brick eine Spalte rechts vom am weitesten rechts liegenden Vorgänger. Bricks ohne Vorgänger ebenfalls in Spalte 0. Zyklen: Rückkanten ignorieren und als Bogen zeichnen.
- **Zeilen:** Bricks einer Spalte untereinander, in der Reihenfolge der `AddNode`-Aufrufe, vertikal um die Mitte verteilt.
- **Maße** (wie in der Vorlage): Brick 190×110, Spaltenabstand 120 (Brick-Abstand x = 310), Zeilenabstand 70 (y-Abstand 180), Netzwerk-Rahmen mit 140 Abstand links und rechts zu den äußeren Bricks, oben Platz für Titel (~130), unten ~40. `viewBox`, `width` und `height` daraus berechnen; Mindestgröße 900×480.
- **Ports an Bricks:** Inport mittig links (bei mehreren gleichmäßig verteilt); Outports gleichmäßig auf der rechten Kante verteilt, in Reihenfolge der Struct-Felder.
- **Brick-Breite bei langen Namen:** Der fette Knotenname (15 px) braucht etwa 9 px pro Zeichen. Ist der
  längste Knotenname breiter als Brick-Breite − 20, alle Bricks gleich breit machen (auf 10 aufrunden, z. B. 280 für
  29 Zeichen wie `createconfluencecustomerspace`). Der Spaltenabstand bleibt 120 (Brick-Abstand x = Breite + 120).
- **Netz-Ports:** Inport links auf dem Rahmen auf Höhe des verbundenen Brick-Inports, Netz-Outports rechts auf dem Rahmen auf Höhe des verbundenen Brick-Outports (mehrere gleichmäßig verteilen, nicht überlappen).
- **Netz-Outport an einem Brick weiter links:** Liegen zwischen dem Brick-Outport und dem rechten Rahmen
  andere Bricks (z. B. `Found` des ersten Bricks einer langen Kette → Netz-Outport), würde eine Linie auf gleicher
  Höhe durch sie hindurchlaufen. Dann den Netz-Outport auf eine freie Höhe über (y ≈ Brick-Oberkante − 60) oder
  unter den Bricks legen und die Linie als Bogen bis kurz vor die nächste Spalte plus Gerade zeichnen:
  `M x1,y1 C xm,y1 xm,yF x2,yF L <Rahmen−10>,yF` (`x2` = Inport-x der nächsten Spalte − 10). Unter dem Titel
  (y ≈ 100) muss Platz bleiben.
- **Linien:** gleiche Höhe → `line`; sonst kubischer Bogen `path d="Mx1,y1 Cxm,y1 xm,y2 x2,y2"` mit `xm` = Mitte zwischen x1 und x2. Start 9 px rechts vom Outport-Kreis, Ende 10 px vor dem Inport-Kreis (Pfeilspitze).

## SVG-Aufbau

Stil und Reihenfolge exakt wie die Vorlage `pkg/components/writerandom.svg` in natsFBP (`/home/dev/Dokumente/Workspaces/solid/natsFBP`). Vorher lesen und dieselben Farben, Schriftgrößen und Elemente verwenden:

1. `<svg xmlns=... viewBox=... width=... height=... font-family="Helvetica, Arial, sans-serif">` mit `<defs>`-Marker `arrow`.
2. Weißer Hintergrund-`rect`.
3. `<!-- Netzwerk <Typ> -->`: gestrichelter Rahmen (`fill="#f4f6fa" stroke="#3b4a6b" stroke-dasharray="8 4"`), fetter Titel `<Typ>`, direkt darunter der **Paketpfad** des Networks (y+20, `font-size="11" fill="#8a96b0"`). Kein `rte.Network`-Text.
4. Je Brick `<!-- Brick <knoten> -->`: weißes `rect` mit `stroke="#222"`, fetter Text = **Knotenname aus `AddNode`** (rect-y+50), grauer Text darunter = **Komponententyp** (rect-y+70, `font-size="12" fill="#666"`), darunter der **Paketpfad** des Komponententyps (rect-y+88, `font-size="10" fill="#999"`).

Paketpfad = `<module aus go.mod>/<Ordner relativ zur Projektwurzel>`, z. B. `natsfbp/pkg/components`. Für Komponenten aus einem anderen Paket den Pfad aus dem Import in der Network-Datei nehmen (z. B. `natsfbp/pkg/other`). Liegt eine Port-Beschriftung an der rechten Brick-Kante auf Höhe dieser Textzeilen, sie über bzw. unter den Port-Kreis schieben.
5. `<!-- Verbindungen -->`, je Verbindung ein Kommentar `<!-- <von>.<Port> -> <nach>.<Port> -->` (bzw. `Netz-Inport`/`Netz-<Name>`) und die Linie mit `stroke="#444" marker-end="url(#arrow)"`.
6. `<!-- Ports: Netzwerk -->` und `<!-- Ports: <knoten> -->`: Kreise r=9, Inport blau (`fill="#4a90d9" stroke="#1f4e85"`), Outport orange (`fill="#e8964a" stroke="#9a5418"`), Beschriftung = exakter Feldname. Offene Ports: Kommentar `<!-- <knoten>.<Port> nicht verbunden -->`, gestrichelte graue Linie (`stroke="#aaa" stroke-dasharray="4 4"`) und Text `offen` in `#999`.
7. Legende unten wie in der Vorlage, inkl. „(Fail/Error-Ports der Bricks nicht dargestellt)“.

Ports und Verbindungen nach den Bricks zeichnen, damit die Kreise über den Rechtecken liegen. Texte dürfen sich nicht überlappen; bei Kollisionen Abstände vergrößern.

## Erzeugen

Ab etwa vier Bricks die Koordinaten nicht von Hand schreiben, sondern ein kleines Python-Skript im
Scratchpad-Verzeichnis anlegen (Liste der Bricks mit Knotenname, Typ und Paket, daraus x-Positionen,
Ports und Linien berechnen) und mit `python3 -I <skript> <ziel.svg>` ausführen. Das Skript gehört nicht ins
Projekt.

## Prüfen

- XML-Gültigkeit prüfen, z. B. `xmllint --noout <datei>.svg` (falls nicht vorhanden: `python3 -c "import xml.dom.minidom,sys; xml.dom.minidom.parse(sys.argv[1])" <datei>.svg`).
- Round-Trip gedanklich nachvollziehen: Liest man das SVG nach den Regeln von `rte-network-from-image`, ergeben sich dieselben Knoten, Typen und Verbindungen wie im Code.
- Optional als PNG rendern (z. B. `rsvg-convert` oder `inkscape`, falls installiert) und das Bild ansehen, um Überlappungen zu finden.
  Auf diesem Rechner sind beide nicht installiert, `cairosvg` fehlt auch. Firefox ist ein Snap und schreibt
  mit `--headless --screenshot` keine Datei nach `/tmp`. Dann nicht weiter probieren, sondern
  Überlappungen über die Koordinaten prüfen (Textbreite ≈ 0,6 × Schriftgröße pro Zeichen) und im Abschluss
  sagen, dass nicht gerendert wurde.

## Abschluss

- Den Go-Code nicht ändern.
- Kurz zusammenfassen: erzeugte SVG (als Link), Knoten mit Typ, Verbindungen, offene Ports, Ergebnis der Prüfung.

