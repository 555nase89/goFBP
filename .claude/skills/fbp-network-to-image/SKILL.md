---
name: fbp-network-to-image
description: Zeichnet aus einem gofbp-Network (fbp.Network aus ../goFBP, Go-Konstruktor mit n.Add/ConnectIn/Connect/ConnectOut) ein SVG-Bild neben der Go-Datei, im Stil der Network-Bilder des Zielprojekts. Auch zum Abgleichen eines bestehenden SVG mit dem Code. Verwenden, wenn der Nutzer ein gofbp-/fbp.Network (z. B. *_net.go) grafisch darstellen, als Bild/Diagramm/SVG haben oder ein vorhandenes Netzwerk-Bild an den Code anpassen will. Für natsFBP-rte.Network stattdessen rte-network-to-image. Aufruf: /fbp-network-to-image <pfad/zur/datei_net.go>
argument-hint: <pfad/zur/datei_net.go>
---

# SVG-Bild aus einem gofbp-Network

Argument: Go-Datei mit einem Network-Konstruktor `func Name(...) (*fbp.Network, error)`, z. B.
`pkg/jira/jiraevents_net.go`. Fehlt es, nach dem Pfad fragen.

Ist es ein natsFBP-`rte.Network` (`this.AddNode`, `this.Con`), stattdessen `rte-network-to-image` nehmen.

## Namen und vorhandene Dateien

- SVG-Datei = Go-Dateiname ohne `_net.go` bzw. `.go` plus `.svg`, im selben Ordner
  (`jiraevents_net.go` → `jiraevents.svg`).
- Titel im Bild = `Name` aus dem `fbp.Network`-Literal (z. B. `jiraevents`), Beschreibungszeile = `Description`.
- Existiert die SVG schon: nicht blind überschreiben. Gegen den Code abgleichen und die Unterschiede
  (Knoten, Verbindungen, Ports) auflisten; anpassen nur nach Rückfrage oder wenn der Nutzer es verlangt
  hat. Beim Anpassen vorher eine Kopie in den Scratchpad legen. Wird ein Knoten eingefügt und der Teil
  rechts davon verschoben, ändern sich **vier** Breiten gemeinsam: Rahmen-`width`, `viewBox`, `width` des
  `<svg>` und `width` des weißen Hintergrund-`rect` (der hat kein `x` und wird beim Verschieben sonst vergessen).
- Mehrere Network-Konstruktoren in der Datei → nachfragen, welcher gemeint ist.

## Go-Code lesen

Aus dem Konstruktor (gofbp, `../goFBP/pkg/fbp/network.go`):

- **Netz-Outports:** immer `Success` und `Error`, dazu die Einträge aus `Outports: []string{...}`
  (Konstanten auflösen, z. B. `CustomerExists`).
- **Knoten:** `n.Add("<knoten>", <ausdruck>)`. Der Ausdruck ist ein Brick-Konstruktor
  (`cmdb.CheckCustomerInCMDB(d.AssetsClient)`) oder ein `*fbp.Network` – ein Unter-Network-Konstruktor
  oder ein Parameter der Funktion (z. B. `createCustomer *fbp.Network`).
  Werden Knoten in einer Schleife über eine Liste angelegt, die Liste auflösen; Reihenfolge = Listenreihenfolge.
- **Verbindungen:**
  - `n.ConnectIn("<knoten>")` → Netz-Inport → Inport des Knotens
  - `n.Connect("<von>", "<outport>", "<nach>")` → Outport → Inport (`fbp.Success`/`fbp.Error` = `Success`/`Error`)
  - `n.ConnectOut("<von>", "<outport>", "<netzOutport>")` → Outport → Netz-Outport
  - In Schleifen jede Iteration einzeln auflösen.
- **Ports je Knoten:** Brick-Funktion bzw. Network-Konstruktor im Zielpaket öffnen und `Outports` lesen.
  Reihenfolge der Outports im Bild: `Success`, dann die Einträge aus `Outports`, `Error` zuletzt.
  Ist der Knoten ein Parameter, die Ports des im Kommentar oder im aufrufenden `cmd/` genannten
  konkreten Networks nehmen (z. B. `service.CreateCustomer`); Ports, die es nur dort gibt, im
  Port-Kommentar kennzeichnen. Unklar → nachfragen, nichts erfinden.
- Outports ohne Verbindung sind „nicht verbunden“.

## Layout

Fluss links → rechts, Spalten nach Tiefe im Graphen (am Netz-Inport = Spalte 0, sonst eine Spalte
rechts vom am weitesten rechts liegenden Vorgänger; Knoten ohne Vorgänger in Spalte 0; Rückkanten als Bogen).
Knoten einer Spalte untereinander in `Add`-Reihenfolge.

- Netzwerk-Rahmen: `x=60 y=40`, rechts auf `x = rechteste Brick-Kante + ~140` (genug Platz für die längste Port-Beschriftung).
- SVG-Breite = Rahmen-Rechtskante + ~100, damit die zentrierten Netz-Port-Labels rechts (z. B. `CustomerExists`)
  nicht über den Rand ragen. Nichts darf über das Bild oder den weißen Hintergrund hinausstehen.
- Brick: Breite 290, erste Spalte bei `x=200`, Spaltenabstand 410 (jiraevents: 510, weil `IssueAttachment` breit ist –
  bei langen Port-Namen den Abstand vergrößern, Beschriftungen dürfen die nächste Linie nicht kreuzen).
- Brick-Höhe: `max(110, 26 * (Anzahl Outports + 1))`; Outports im Abstand 26 ab `y + 26`, Inport mittig links
  bzw. auf Höhe des vorgelagerten Outports, damit die Linie gerade wird.
- Hauptkette auf einer Höhe (oben ~y=200), Titelblock oben (Titel y=68, Paket y=88, Beschreibung y=104).
- Zusätzliche Netz-Outports (z. B. `CustomerExists`) oben auf einer eigenen Schiene (y≈140), `Success` rechts auf
  Höhe des verbundenen Outports, `Error` unten auf einer Sammelschiene ~40 px unter dem tiefsten Brick.
  Rahmen-Unterkante ~30 px unter der Error-Schiene, Legende ~40 px unter dem Rahmen; SVG-Höhe ~25 px darunter.
- Linien: gleiche Höhe → `line`; sonst `path d="Mx1,y1 Cxm,y1 xm,y2 x2,y2"` (xm = Mitte), Start 9 px rechts vom
  Outport-Kreis, Ende 10 px vor dem Ziel-Kreis. Zur Error-Schiene: `M x1,y C x1+40,y x1+40,rail x1+80,rail L x2,rail`.

## SVG-Aufbau

Stil exakt wie ein bestehendes Network-Bild (`*.svg`) des Zielprojekts (vorher lesen); Reihenfolge:

1. `<svg xmlns viewBox width height font-family="Helvetica, Arial, sans-serif">`, `<defs>` mit Markern
   `arrow` (`#444`) und `arrow-err` (`#c0392b`); weißer Hintergrund-`rect` mit genau der Breite und Höhe der `viewBox`.
2. `<!-- Network <name> -->`: Rahmen `rx="10" fill="#f4f6fa" stroke="#3b4a6b" stroke-width="2" stroke-dasharray="8 4"`,
   Titel (`font-size="18" font-weight="bold" fill="#3b4a6b"`), Paketpfad (`11`, `#8a96b0`),
   Beschreibung (`12`, italic, `#666`).
3. Je Knoten `<!-- Brick <knoten> -->`: `rect rx="6"`, Texte zentriert (`text-anchor="middle"`) bei rect-y+42/60/76/92:
   Knotenname (15, bold), Brick-Name/Typ (12, `#666`), Paketpfad (10, `#999`), Konstruktoraufruf
   (10, `#999`, `font-family="monospace"`, nach 44 Zeichen mit `…` kürzen).
   Brick: `fill="#ffffff" stroke="#222" stroke-width="2"`. Network-Knoten: `fill="#f4f6fa" stroke="#3b4a6b"
   stroke-dasharray="6 3"`, Typzeile `*fbp.Network` (bei Parametern `*fbp.Network (parameter <name>)`,
   Konstruktorzeile `e.g. <paket>.<Konstruktor>(...)`).
   Paketpfad = `<module aus go.mod>/<ordner>`, z. B. `example.com/app/pkg/cmdb`.
4. `<!-- Connections -->`, je Verbindung `<!-- <von>.<Port> -> <nach>.Inport -->` bzw. `Netz-Inport`/`Netz-<Name>`.
   Normal: `stroke="#444" stroke-width="2" fill="none" marker-end="url(#arrow)"`;
   Error: `stroke="#c0392b" stroke-width="1.5" marker-end="url(#arrow-err)"`.
5. `<!-- Ports: Network -->`, dann `<!-- Ports: <knoten> -->`: Kreise r=9 — Inport `#4a90d9`/`#1f4e85`,
   Outport `#e8964a`/`#9a5418`, Error `#e06666`/`#8e2b27`. Brick-Labels `font-size="11"`: Inport links
   (`x-13`, `y-10`, `text-anchor="end"`), Outports rechts (`x+13`, `y-8`). Netz-Port-Labels `font-size="12"`
   zentriert 17 px über dem Kreis. Unverbundene Ports: nur Kommentar `<!-- <knoten>.<Port> nicht verbunden -->`
   vor dem Kreis, keine Stummel.
6. `<!-- Legende -->` wie in der Vorlage: Inport, Outport, Error-Outport, Brick, Network.

Ports nach Bricks und Linien zeichnen (Kreise liegen oben). Texte dürfen sich nicht überlappen.

## Prüfen

- `python3 -c "import xml.dom.minidom,sys; xml.dom.minidom.parse(sys.argv[1])" <datei>.svg` (oder `xmllint --noout`).
- Ränder: `viewBox`, `<svg width/height>` und Hintergrund-`rect` sind gleich groß; kein `x`/`cx`/Pfadpunkt liegt
  rechts davon, und bei zentrierten Labels reicht `x + halbe Textbreite` (~3,5 px je Zeichen bei font-size 12) nicht darüber.
- Abgleich: jede `Add`/`Connect*`-Zeile hat genau ein Element im Bild und umgekehrt; vergleichen über die
  Kommentare (`grep -o '<!--.*-->'`).
- Falls `rsvg-convert`/`inkscape` vorhanden: als PNG rendern und ansehen. Sonst sagen, dass nur über Koordinaten geprüft wurde.
- Andere SVGs, die dasselbe Network zeigen (z. B. als Knoten), auf Widersprüche zum Code prüfen und melden.

## Abschluss

- Go-Code nicht ändern.
- Kurz berichten: SVG als Link, Knoten mit Typ, Verbindungen, offene Ports, Prüfergebnis, gefundene Abweichungen.
