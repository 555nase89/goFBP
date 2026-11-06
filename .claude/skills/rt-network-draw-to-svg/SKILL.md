---
name: rt-network-draw-to-svg
description: Wandelt eine Skizze (Foto/Scan, z.B. .jpg/.png) eines natsfbp-Netzwerks (rte.Network mit Bricks, Ports und Verbindungen) in ein SVG im einheitlichen Stil der SVGs in natsFBP/pkg/components um. Verwenden, wenn der Nutzer ein Bild eines FBP-/rte-Netzwerks als SVG haben möchte oder ein Netzwerk-Diagramm für einen Brick zeichnen lassen will.
---

# Netzwerk-Skizze → SVG

Ziel: Aus einer handgezeichneten Skizze eines `rte.Network` ein SVG bauen, das genauso aussieht wie die
bestehenden Diagramme (z.B. `natsFBP/pkg/components/writerandom.svg`, `writer2324.svg`).
Das SVG liegt neben dem Bild und hat denselben Basisnamen (`foo.jpg` → `foo.svg`).

## Ablauf

1. **Bild finden.** Pfade sind case-sensitiv (`natsFBP`, nicht `natsfbp`). Bei Bedarf mit
   `find . -iname "<name>*"` suchen.
2. **Bild verkleinern**, bevor du es ansiehst (Handyfotos haben oft mehrere MB). ImageMagick ist
   nicht installiert, daher PIL:
   ```bash
   python3 -I -c "from PIL import Image,ImageOps;im=ImageOps.exif_transpose(Image.open('<bild>'));im.thumbnail((1600,1600));im.save('<scratchpad>/sketch.jpg')"
   ```
   Danach das verkleinerte Bild mit dem Read-Tool ansehen.
3. **Orientierung klären.** Skizzen sind oft um 90° gedreht fotografiert. Maßgeblich ist die
   Leserichtung der Beschriftung: Der Netzwerkname steht oben links, der Datenfluss läuft von links
   (Inport) nach rechts (Outports).
4. **Skizze lesen:**
   - äußeres abgerundetes Rechteck = Netzwerk, Beschriftung = Netzwerkname
   - innere Kästen = Bricks (Beschriftung = Klassenname)
   - kleine Kreise = Ports (am Netzwerkrand: Netzwerk-Ports; am Brick: Brick-Ports)
   - Linien = Verbindungen; Beschriftungen an Linien/Ports = Portnamen
5. **Mit dem Code abgleichen.** Nach den Klassen suchen
   (`grep -rli "<klassenname>" --include=*.go ..`), z.B. auch in Nachbarprojekten.
   - Handschriftliche Portnamen gegen die echten Outport-Felder prüfen und korrigieren.
   - Outports, die der Brick im Code hat, die aber in der Skizze fehlen, als **offen** einzeichnen.
   - Ist das Netzwerk schon implementiert, gilt die Verdrahtung in `Init()`
     (`AddNode` / `ConInport` / `Con` / `ConOutport`) vor der Skizze. Widersprüche dem Nutzer melden.
   - Knotennamen (fett im Brick) = Name aus `AddNode`, sonst der kleingeschriebene Klassenname.
6. **SVG schreiben** nach der Vorlage und den Layoutregeln unten.
7. **Prüfen:** Wohlgeformtheit mit
   `python3 -I -c "import xml.dom.minidom as m;m.parse('<datei>.svg');print('ok')"`.
   Einen SVG-Renderer gibt es nicht (`rsvg-convert` und `cairosvg` fehlen). Deshalb Koordinaten von
   Hand auf Überlappungen prüfen und dem Nutzer sagen, dass er das Bild selbst ansehen soll.
8. **Annahmen melden:** z.B. geratene Portnamen (unbeschriftete Outports heißen standardmäßig
   `Success`), ergänzte offene Ports, unleserliche Handschrift.

## Stil (verbindlich)

| Element | Werte |
|---|---|
| Canvas | `viewBox="0 0 900 480"`, `width="900" height="480"`, `font-family="Helvetica, Arial, sans-serif"`, weißer Hintergrund-`rect` |
| Netzwerk | `rect x=60 y=40 width=780 height=400 rx=10 fill=#f4f6fa stroke=#3b4a6b stroke-width=2 stroke-dasharray="8 4"` |
| Netzwerk-Titel | `x=80 y=68`, 18px, bold, `#3b4a6b`; darunter `x=80 y=88`, 12px, `#6b7a99`, Text `rte.Network` |
| Brick | `rect width=190 rx=6 fill=#ffffff stroke=#222 stroke-width=2`, Höhe 110 (mehr bei vielen Ports) |
| Brick-Text | Knotenname 15px bold, zentriert; darunter Klassenname 12px `#666`, 20px tiefer |
| Inport | `circle r=9 fill=#4a90d9 stroke=#1f4e85 stroke-width=2`, auf dem linken Rand |
| Outport | `circle r=9 fill=#e8964a stroke=#9a5418 stroke-width=2`, auf dem rechten Rand |
| Portlabel Brick | 11px; Inport: `x=cx+12`, `y=cy-20`; Outports: `text-anchor="end"`, `x=cx-12`, `y=cy+4` (bei nur einem Outport auch `y=cy-20`) |
| Portlabel Netzwerk | 12px, `text-anchor="middle"`, 17px über dem Kreis (Inport `x=52`, Outport `x=848`) |
| Verbindung | `stroke=#444 stroke-width=2 marker-end="url(#arrow)"`; beginnt 9px hinter dem Quellport, endet 10px vor dem Zielport. Gerade `line` bei gleicher Höhe, sonst kubischer `path` `M x1,y1 C xm,y1 xm,y2 x2,y2` |
| Offener Port | `line` 21px lang, `stroke=#aaa stroke-width=2 stroke-dasharray="4 4"`, dahinter Text `offen` 11px `#999` |
| Legende | unverändert aus der Vorlage übernehmen (y=462/466) |

Optional (wie in `writerandom.svg`): Paketpfad als dritte Zeile im Brick (10px, `#999`) bzw. statt
`rte.Network` unter dem Netzwerktitel (11px, `#8a96b0`).

## Layoutregeln

- Spalten für Bricks von links nach rechts: x=200, x=510 (bei drei Stufen z.B. 170 / 390 / 610 und
  Abstände entsprechend anpassen). Netzwerk-Inport bei x=60, Netzwerk-Outports bei x=840.
- Mehrere Outports an einem Brick im Abstand von 32–36px. Bei mehr als zwei Outports den Brick höher
  machen und den Text **oben** in den Brick setzen (Titel `top+30`, Klasse `top+48`), damit er nicht
  mit den Portlabels kollidiert.
- Wenn möglich verbundene Ports auf gleiche Höhe legen, damit die Verbindungen gerade Linien sind.
- Parallele Zweige (ein Outport → mehrere Bricks) übereinander stapeln (z.B. Bricks bei y=110 und y=290).
- Alles muss im Netzwerk-Rechteck (y 40–440) bleiben. Labels dürfen sich nicht überlappen
  (11px Text ≈ 6px pro Zeichen).
- Kommentare im SVG auf Deutsch, Abschnittsreihenfolge wie in der Vorlage: Netzwerk, Bricks,
  Verbindungen, Ports Netzwerk, Ports je Brick, Legende.

## Vorlage

```xml
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 480" width="900" height="480" font-family="Helvetica, Arial, sans-serif">
  <defs>
    <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0,0 L10,5 L0,10 z" fill="#444"/>
    </marker>
  </defs>

  <rect width="900" height="480" fill="#ffffff"/>

  <!-- Netzwerk NETZNAME -->
  <rect x="60" y="40" width="780" height="400" rx="10" fill="#f4f6fa" stroke="#3b4a6b" stroke-width="2" stroke-dasharray="8 4"/>
  <text x="80" y="68" font-size="18" font-weight="bold" fill="#3b4a6b">NETZNAME</text>
  <text x="80" y="88" font-size="12" fill="#6b7a99">rte.Network</text>

  <!-- Brick knotena -->
  <rect x="200" y="170" width="190" height="110" rx="6" fill="#ffffff" stroke="#222" stroke-width="2"/>
  <text x="295" y="220" font-size="15" font-weight="bold" text-anchor="middle">knotena</text>
  <text x="295" y="240" font-size="12" fill="#666" text-anchor="middle">KlasseA</text>

  <!-- Brick knotenb -->
  <rect x="510" y="170" width="190" height="110" rx="6" fill="#ffffff" stroke="#222" stroke-width="2"/>
  <text x="605" y="220" font-size="15" font-weight="bold" text-anchor="middle">knotenb</text>
  <text x="605" y="240" font-size="12" fill="#666" text-anchor="middle">KlasseB</text>

  <!-- Verbindungen -->
  <!-- Netz-Inport -> knotena.Inport -->
  <line x1="69" y1="225" x2="190" y2="225" stroke="#444" stroke-width="2" marker-end="url(#arrow)"/>
  <!-- knotena.Success -> knotenb.Inport -->
  <line x1="399" y1="225" x2="500" y2="225" stroke="#444" stroke-width="2" marker-end="url(#arrow)"/>
  <!-- knotenb.Success -> Netz-Success -->
  <line x1="709" y1="225" x2="830" y2="225" stroke="#444" stroke-width="2" marker-end="url(#arrow)"/>

  <!-- Ports: Netzwerk -->
  <circle cx="60" cy="225" r="9" fill="#4a90d9" stroke="#1f4e85" stroke-width="2"/>
  <text x="52" y="208" font-size="12" text-anchor="middle">Inport</text>
  <circle cx="840" cy="225" r="9" fill="#e8964a" stroke="#9a5418" stroke-width="2"/>
  <text x="848" y="208" font-size="12" text-anchor="middle">Success</text>

  <!-- Ports: knotena -->
  <circle cx="200" cy="225" r="9" fill="#4a90d9" stroke="#1f4e85" stroke-width="2"/>
  <text x="212" y="205" font-size="11">Inport</text>
  <circle cx="390" cy="225" r="9" fill="#e8964a" stroke="#9a5418" stroke-width="2"/>
  <text x="378" y="205" font-size="11" text-anchor="end">Success</text>

  <!-- Ports: knotenb -->
  <circle cx="510" cy="225" r="9" fill="#4a90d9" stroke="#1f4e85" stroke-width="2"/>
  <text x="522" y="205" font-size="11">Inport</text>
  <circle cx="700" cy="225" r="9" fill="#e8964a" stroke="#9a5418" stroke-width="2"/>
  <text x="688" y="205" font-size="11" text-anchor="end">Success</text>
  <!-- Beispiel offener Port:
  <line x1="709" y1="345" x2="730" y2="345" stroke="#aaa" stroke-width="2" stroke-dasharray="4 4"/>
  <text x="734" y="349" font-size="11" fill="#999">offen</text>
  -->

  <!-- Legende -->
  <circle cx="80" cy="462" r="7" fill="#4a90d9" stroke="#1f4e85" stroke-width="2"/>
  <text x="93" y="466" font-size="12">Inport</text>
  <circle cx="160" cy="462" r="7" fill="#e8964a" stroke="#9a5418" stroke-width="2"/>
  <text x="173" y="466" font-size="12">Outport</text>
  <rect x="240" y="454" width="22" height="16" rx="3" fill="#fff" stroke="#222" stroke-width="2"/>
  <text x="270" y="466" font-size="12">Brick</text>
  <line x1="320" y1="462" x2="345" y2="462" stroke="#aaa" stroke-width="2" stroke-dasharray="4 4"/>
  <text x="351" y="466" font-size="12">nicht verbunden</text>
  <text x="460" y="466" font-size="12" fill="#888">(Fail/Error-Ports der Bricks nicht dargestellt)</text>
</svg>
```

Fail/Error-Ports der Bricks werden nie gezeichnet (siehe Legende), ebenso der `stop`-Endpoint.
