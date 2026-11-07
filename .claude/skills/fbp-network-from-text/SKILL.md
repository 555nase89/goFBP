---
name: fbp-network-from-text
description: Baut aus einer kurzen Pfeil-Notation (IN > brick.Outport > brick > Success) einen gofbp-Network-Konstruktor (fbp.Network aus ../goFBP) als <name>_net.go, mit Test, bei Services cmd/<name>/, und zeichnet danach das SVG mit fbp-network-to-image. Der Name kommt aus dem Dateinamen der .fbp-Datei. Verwenden, wenn der Nutzer ein Network als Text, Pfeile, Skizze oder .fbp-Datei beschreibt. Aufruf: /fbp-network-from-text <pfad/zur/datei.fbp>
argument-hint: <pfad/zur/datei.fbp>
---

# gofbp-Network aus Pfeil-Notation

Argument: Pfad zu einer Textdatei `<name>.fbp`, z. B. `pkg/record/record.fbp`. Sie liegt dort, wo das
`<name>_net.go` hin soll, und wird versioniert: Sie ist die Quelle des Networks.
Schreibt der Nutzer die Notation direkt in den Chat, nach Ordner und Namen fragen (falls nicht klar) und den Text
zuerst als `<name>.fbp` speichern.

## Notation

```
description: writes Events and log records of goFBP Services to the store
storeevent = StoreEvent(st)

IN  > checkevent > storeevent > Success
Log > checklog   > storelog   > Success
checkevent.Invalid > Rejected
*.Error > Error
```

- **Eine Zeile = eine Kette.** `>` verbindet ein Element mit dem nächsten.
- **Bricks klein** (`checkevent`), **Netzwerk-Ports mit Großbuchstaben** (`IN`, `Log`, `Success`, `Error`, `Rejected`).
- **`brick.Outport > ziel`**: Verbindung vom Outport des Bricks. **Ohne `.Outport` ist `Success` gemeint**:
  `checkevent > storeevent` = `checkevent.Success > storeevent`. In einer Kette gilt das für jedes Glied.
- **Erstes Element ein Netzwerk-Port:** `IN` (auch `In`) = Standard-Inport → `n.ConnectIn`; jeder andere = eigener
  Inport → `Inports` + `n.ConnectInport`.
- **Letztes Element ein Netzwerk-Port:** `Success`/`Error` → `n.ConnectOut(.., fbp.Success/fbp.Error)`; jeder andere =
  eigener Outport → `Outports` + `n.ConnectOut`.
- **Anderer Inport des Ziel-Bricks:** `Inport:brick`, z. B. `a.Out > Left:join` → `n.ConnectTo("a", "Out", "join", "Left")`.
  Ohne Angabe: Inport `In`.
- **`*.Error > Error`**: der `Error` jedes Knotens geht auf den Netzwerk-`Error`. Analog `*.<Outport> > <Port>` für
  alle Knoten, die diesen Outport haben.
- **Fan-out:** mehrere Zeilen mit demselben Anfang (`IN > a` und `IN > b`).
- **`description: ...`** → `Description` des Networks. Fehlt sie, eine kurze englische formulieren und am Ende nennen.
- **`knoten = Konstruktor(args)`** (optional): welcher Brick bzw. welches Unter-Network hinter einem Knoten steckt.
  Ohne Zeile: der Konstruktor im Projekt, dessen Name kleingeschrieben dem Knotennamen entspricht
  (`checkevent` → `CheckEvent()`); seine Parameter werden Parameter des Network-Konstruktors.
- Leerzeilen und `#`-Kommentare werden ignoriert.

Ist eine Zeile nicht eindeutig (z. B. Element mit Großbuchstaben mitten in der Kette, unbekannter Knoten ohne
Konstruktor), nicht raten, sondern in der gesammelten Rückfrage klären.

## Ablauf

Wie [fbp-network-from-image](../fbp-network-from-image/SKILL.md), nur dass die Knoten und Verbindungen aus der
`.fbp`-Datei statt aus dem SVG kommen. Von dort gelten unverändert: **Namen** (Dateiname ohne `.fbp` statt ohne
`.svg`), **Projekt und Konventionen bestimmen**, **Prüfungen vor dem Anlegen**, **Go-Datei des Networks**, **Test**,
**cmd-Ordner**, **Prüfen und bauen**, **Dokumentation**, **Abschluss**.

Abweichungen:

- Reihenfolge der `n.Add` = erstes Auftreten des Knotens in der Datei.
- Existiert `<name>_net.go` schon, nicht stumpf überschreiben: das Network gegen die Datei abgleichen, die
  Unterschiede (neue/entfernte Knoten und Verbindungen) zeigen und nach Bestätigung nur diese ändern. Test anpassen.
- **Zum Schluss das Bild:** mit `fbp-network-to-image` aus dem fertigen `<name>_net.go` das `<name>.svg` daneben
  erzeugen bzw. aktualisieren. Dabei Round-Trip prüfen: Knoten und Verbindungen im Bild = Zeilen der `.fbp`-Datei.
- In der Zusammenfassung zusätzlich die `.fbp`-Datei und das SVG verlinken.
