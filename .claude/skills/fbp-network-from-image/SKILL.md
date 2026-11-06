---
name: fbp-network-from-image
description: Baut aus einem SVG-Bild eines FBP-Netzwerks einen gofbp-Network-Konstruktor (fbp.Network aus ../goFBP, n.Add/ConnectIn/Connect/ConnectOut) als <name>_net.go neben dem Bild, mit Test, und – wenn das Network als Service laufen soll – cmd/<name>/ mit main.go, example.json, config.json und gebautem Binary. Der Name kommt immer aus dem Dateinamen der SVG. Gegenstück zu fbp-network-to-image. Verwenden, wenn der Nutzer aus einem Bild/Diagramm/SVG ein gofbp-Network, Netzwerk oder einen Service erstellen will (z. B. in goFBP). Für natsFBP-rte.Network stattdessen rte-network-from-image. Aufruf: /fbp-network-from-image <pfad/zum/bild.svg>
argument-hint: <pfad/zum/bild.svg>
---

# gofbp-Network aus einem Bild

Argument: Pfad zu einer SVG-Datei, z. B. `pkg/service/createcustomer.svg`.
Fehlt es, nach dem Pfad fragen.

Ablauf: SVG lesen → Projekt und Konventionen bestimmen → **alle Prüfungen** → offene Fragen gesammelt stellen →
erst dann Dateien anlegen.

Ist das Projekt ein natsFBP-Projekt (`rte.Network`, `AddNode`), stattdessen `rte-network-from-image` nehmen.

## Namen

Der Name kommt **immer aus dem Dateinamen der SVG** ohne `.svg`; der Titel im Bild dient nur zur Kontrolle.

| Verwendung | Regel | Beispiel `addhellocomment.svg` |
|---|---|---|
| `Name` im `fbp.Network` | klein wie der Dateiname | `"addhellocomment"` |
| Konstruktor | erster Buchstabe groß, Rest unverändert (CamelCase nur, wenn der Nutzer es sagt) | `Addhellocomment` |
| Go-Datei / Test | `<name>_net.go` / `<name>_net_test.go` neben der SVG | `pkg/service/addhellocomment_net.go` |
| cmd-Ordner, Binary | klein (nur bei Services) | `cmd/addhellocomment/addhellocomment` |

Weicht der Titel im Bild vom Dateinamen ab, am Ende erwähnen.

## Projekt und Konventionen bestimmen

- Projektwurzel = nächster Ordner oberhalb der SVG mit `go.mod`; Modulname aus der `module`-Zeile.
- **`CLAUDE.md` des Projekts lesen**, dazu `../goFBP/docs/GLOSSARY.md` (Begriffe und Regeln). Projektregeln haben
  Vorrang vor diesem Skill.
- Paket = Paket der anderen `.go`-Dateien im Ordner der SVG.
- **Service oder nicht:**
  - Projekte mit `pkg/service`: Networks in `pkg/service` laufen als Service und bekommen `cmd/<name>/`. Networks in Fachpaketen
    (z. B. `pkg/jira`) dienen nur als Knoten und bekommen **kein** cmd. Liegt die SVG in einem Fachpaket, der
    Nutzer will aber einen Service, fragen, ob das Network nach `pkg/service` soll.
  - goFBP: Beispiele unter `examples/<name>/` mit Konstante `PackagePath` und `examples/<name>/cmd/<name>/`.
- Vorlagen **im Zielprojekt** suchen: das ähnlichste bestehende `*_net.go` (z. B. `pkg/service/addhellocomment_net.go`,
  mit vielen Abhängigkeiten `createcustomer_net.go` mit `Deps`), der zugehörige Test und das passende `cmd/<x>/main.go`.
  Sonst die aus goFBP (`examples/text/text.go`, `examples/text/cmd/textservice/main.go`).
- Existieren Go-Datei, Test oder cmd-Ordner schon, nicht überschreiben, sondern fragen.

## SVG lesen

Bilder im Stil von `fbp-network-to-image` (Kommentare im SVG helfen: `<!-- Brick x -->`, `<!-- a.Port -> b.Inport -->`,
`<!-- x.Port nicht verbunden -->`):

- **Network:** gestrichelter Rahmen; fetter Titel, darunter Paketpfad (nur Kontrolle; weicht er vom Ablageort ab,
  am Ende erwähnen), kursive Zeile = `Description`.
- **Netz-Ports:** blauer Kreis links = Inport; orange Kreise rechts = Outports, rot = `Error`. Jeder Outport außer
  `Success` und `Error` kommt in `Outports` (üblich als exportierte Konstante, wie `CustomerExists`).
- **Knoten:** Rechteck mit fettem Knotennamen (für `n.Add`), grauer Brick-Name, Paketpfad, optional eine
  Monospace-Zeile mit dem Konstruktoraufruf (Argumente daraus übernehmen; `…` = gekürzt, im Code nachsehen).
  Gestricheltes Rechteck = verschachteltes Network: Unter-Network-Konstruktor oder – bei
  `*fbp.Network (parameter <name>)` – ein Parameter des neuen Konstruktors.
- **Ports eines Knotens:** blau = Inport, orange = Outport, rot = `Error`; Beschriftung = Outport-Name.
- **Verbindungen:** `line`/`path` mit Pfeil vom Outport zum Inport bzw. zum Netz-Port. Rote Linien zur Error-Schiene
  = `ConnectOut(<knoten>, fbp.Error, fbp.Error)`.
- Unverbundene Ports (nur Kommentar, oder grau gestrichelt mit „offen“): keinen Code erzeugen.

## Prüfungen vor dem Anlegen

1. **Bricks finden:** jede Brick-Funktion `func <Name>(...) *fbp.Brick` bzw. jeden Network-Konstruktor im Projekt
   und in Nachbarmodulen suchen (`grep -rn "func <Name>(" --include=*.go`). Prüfen, dass jeder verwendete Outport
   existiert (`Success`, `Error` oder in `Outports`), und die Parameter notieren.
2. **Abhängigkeitsrichtung:** Liegt ein Brick in einem anderen Modul oder Paket, prüfen, dass der Import keinen
   Zyklus erzeugt (`replace`-Richtung in `go.mod`, z. B. importiert `pkg/jira` nicht `pkg/service` –
   dann wird der Knoten zum Parameter `*fbp.Network`, wie `createCustomer` in `jira.JiraEvents`). Sonst Verlegen
   vorschlagen und fragen.
3. **Fehlende Bricks:** nichts erfinden. Ähnlichsten Brick nennen (Ports, Payload) und anbieten:
   **a)** neuen Brick auf dessen Basis bauen (Brick-Muster der Projekt-`CLAUDE.md`, Test, Eintrag in `docs/bricks.md`),
   **b)** den bestehenden verwenden und sagen, was sich an Payload oder Verhalten ändert.
4. **Namen:** `Name` muss innerhalb des Pakets eindeutig sein (bildet das Service-Subject
   `fbp.<packagepath>.<name>`); kein anderes Network im Paket mit demselben `Name`, keine gleichnamige Go-Funktion.
   Knotennamen eindeutig und klein; das Network darf sich nicht selbst (auch nicht indirekt) enthalten.
5. **Projektregeln gegen das Bild:** z. B. müssen in manchen Projekten die `Error`-Outports **aller** Knoten mit `Error` des
   Networks verbunden sein. Fehlt das im Bild, nachfragen oder nach Regel ergänzen und am Ende nennen.

Alle offenen Fragen aus 2–5 möglichst in **einer** Rückfrage stellen.

## Go-Datei des Networks

Aufbau wie die Vorlage im Projekt:

- Kommentar über dem Konstruktor (Englisch): was das Network tut, wohin `Success`/`Error` gehen, welche Outports
  offen bleiben.
- Signatur `func <Konstruktor>(<abhängigkeiten>) (*fbp.Network, error)`. Abhängigkeiten = Parameter der
  Brick-Konstruktoren (z. B. `jiraClient *jiraclient.Client`) und Network-Parameter. Bei vielen ein `Deps`-Struct
  nach dem Muster von `service.Deps`, bestehende Config-Helfer (`NewDeps`, `JiraConfig.Client()`) wiederverwenden.
- `n := &fbp.Network{Name: "<name>", Description: "<englisch, kurz>", Outports: []string{...}}`
- Ein `errors.Join(...)` mit, in dieser Reihenfolge: `n.Add` je Knoten (Reihenfolge wie im Bild von links nach
  rechts), `n.ConnectIn`, `n.Connect(von, outport, nach)`, `n.ConnectOut(von, outport, netzOutport)`.
  `fbp.Success`/`fbp.Error` statt String-Literalen.
- Lange gleichförmige Ketten dürfen als Schleife über eine Knotenliste geschrieben werden (wie `CreateCustomer`).
- Optional `Start<Konstruktor>`, wenn das Projekt das für Nicht-Service-Networks so macht (`jira.StartJiraEvents`).

## Test

`<name>_net_test.go` im Stil des Projekts: Network in-process mit `n.Start(ctx, fbp.WithOutput(...))` starten
(z. B. Hilfen `start`/`next` aus `pkg/service/service_test.go`), externe APIs mit `httptest.NewServer` und
echtem Client. Mindestens: ein Packet erreicht `Success`, ein fehlerhaftes `Error`, ein unverbundener Pfad liefert
keine Ausgabe. Für Services den Namen in `TestSubjects` ergänzen.

## cmd-Ordner (nur Services)

1. `main.go` als Kopie der `main.go` mit den passenden Config-Blöcken (z. B. `addhellocomment` für `jira`,
   `createcustomer` für `jira`/`confluence`). Nur Paket-Kommentar (Inport-Subject
   `fbp.<packagepath>.<name>.ip.in`), Config-Struct, Konstruktoraufruf und Namen in `slog`/`natscon.Connect` ändern.
   Danach per `diff` gegen die Vorlage kontrollieren.
2. `example.json` (versioniert, ohne Secrets) nach der Vorlage mit denselben Blöcken wie das Config-Struct.
3. `config.json` (nicht versioniert) per `cp` aus dem Vorlagen-cmd kopieren, **ohne den Inhalt auszugeben**; am Ende
   erwähnen, dass darin echte Tokens mitkopiert sein können. Fehlen Blöcke, aus `example.json` ergänzen.
4. Gibt es im Projekt ein `./compose.sh`, es danach ausführen (erzeugt `docker-compose.yml` aus `cmd/`) und prüfen, ob
   `docs/nats-server.conf.example` die neuen Subjects schon abdeckt.

## Prüfen und bauen

Go immer über `/snap/go/current/bin/go`, aus der Projektwurzel:

```bash
gofmt -l <geänderte ordner>
/snap/go/current/bin/go vet ./...
/snap/go/current/bin/go test ./<paketordner>/...
/snap/go/current/bin/go build -o cmd/<name>/<name> ./cmd/<name>/   # nur bei Services
```

Fehler beheben, bis alles ohne Meldung durchläuft. Zur Kontrolle des Round-Trips kann mit `fbp-network-to-image`
gedanklich (oder in eine Scratchpad-Datei) aus dem neuen Code ein Bild erzeugt und mit der SVG verglichen werden.

## Dokumentation

Wie die Projekt-`CLAUDE.md` verlangt, im selben Zug pflegen: falls vorhanden `docs/bricks.md` (neue Networks und
Bricks) und die Strukturtabelle in `CLAUDE.md`; in goFBP `tasks/todo.md` („Done“).

## Abschluss

- `.gitignore` nicht anfassen; prüfen, dass `config.json` und das Binary ignoriert sind, sonst darauf hinweisen.
- Den Service nicht starten, aber am Ende anbieten.
- Kurz zusammenfassen: angelegte Dateien (als Links), Knoten und Verbindungen, offene Ports, Abweichungen vom Bild
  (Name, Ablageort, ergänzte Error-Verbindungen, Parameter statt Import), Ergebnis von gofmt/vet/test/build.
