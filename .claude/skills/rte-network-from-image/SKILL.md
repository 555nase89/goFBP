---
name: rte-network-from-image
description: Baut aus einem SVG-Bild eines FBP-Netzwerks ein rte.Network (natsfbp) als Go-Datei neben dem Bild und legt dazu cmd/<name>/ mit config.json, main.go und gebautem Binary an. Der Name kommt immer aus dem Dateinamen der SVG. Verwenden, wenn der Nutzer aus einem Bild/Diagramm/SVG ein rte.Network, Netzwerk oder einen Dienst erstellen will. Aufruf: /rte-network-from-image <pfad/zum/bild.svg>
argument-hint: <pfad/zum/bild.svg>
---

# rte.Network aus einem Bild

Argument: Pfad zu einer SVG-Datei, z. B. `pkg/components/writerandom23.svg`.
Fehlt es, den Nutzer nach dem Pfad fragen.

Ablauf: SVG lesen → Projekt und Konventionen bestimmen → **alle Prüfungen** (Komponenten, Abhängigkeitsrichtung,
Namenskollision, vorhandene Dateien) → offene Fragen gesammelt stellen → erst dann Dateien anlegen.

## Namen

Der Name kommt **immer aus dem Dateinamen der SVG** ohne `.svg`. Der Name im Bild selbst wird ignoriert.

| Verwendung | Regel | Beispiel für `writerandom23.svg` |
|---|---|---|
| Go-Typ | erster Buchstabe groß, Rest unverändert | `Writerandom23` |
| Go-Datei | klein, neben der SVG (Projektkonvention beachten, z. B. `<name>_net.go`) | `pkg/components/writerandom23.go` |
| cmd-Ordner | klein (Projektkonvention beachten, z. B. `cmd/jira/<name>/`) | `cmd/writerandom23/` |
| Binary | klein, wie der Ordner | `cmd/writerandom23/writerandom23` |
| Variable in main.go | klein | `writerandom23` |

Weicht der Name im Bild vom Dateinamen ab, das am Ende kurz erwähnen.

## Projekt und Konventionen bestimmen

- Projektwurzel = nächster Ordner oberhalb der SVG mit einer `go.mod`; Modulname aus deren `module`-Zeile lesen.
- **Die `CLAUDE.md` des Projekts lesen.** Ihre Konventionen haben Vorrang vor diesem Skill und vor den
  natsFBP-Vorlagen: Dateiname von Networks (z. B. `pkg/jira/*_net.go`), Ort der cmd-Ordner (z. B.
  `cmd/jira/<brick>/`), Sprache der Kommentare (z. B. Englisch), Config-Struktur in `main.go`, Testpflicht.
- Paket der Go-Datei = Paket der anderen `.go`-Dateien im Zielordner (meist `components`).
- Vorlagen **zuerst im Zielprojekt** suchen: ein bestehendes rte.Network und ein bestehendes `cmd/.../main.go`, das
  einen Brick startet. Nur wenn es dort keine gibt, die aus natsFBP nehmen
  (`/home/dev/Dokumente/Workspaces/solid/natsFBP`: `pkg/components/writerandom.go`, `cmd/writerandom/main.go`) und
  Importpfade anpassen.
- Existiert die Go-Datei oder der cmd-Ordner schon, nicht überschreiben, sondern den Nutzer fragen.

## SVG lesen

Die Bilder folgen dieser Konvention (Kommentare im SVG helfen zusätzlich):

- **Netzwerk:** gestrichelter Rahmen; blaue Kreise am Rand = Netz-Inports, orange Kreise = Netz-Outports, Beschriftung daneben = Portname.
- **Brick (Knoten):** Rechteck mit fettem Text = Knotenname (für `AddNode`) und grauem Text darunter = Komponententyp (Go-Typ). Eine optionale dritte, kleinere Zeile darunter (z. B. `natsfbp/pkg/components`) = Paketpfad des Typs: liegt er im selben Paket wie die neue Go-Datei, Typ ohne Präfix verwenden; sonst das Paket importieren und `<paket>.<Typ>` schreiben.
- **Paketpfad des Netzwerks:** optionale kleine Zeile unter dem Netzwerknamen im Rahmen (ältere Bilder haben dort stattdessen bzw. zusätzlich `rte.Network`, das ignorieren); nur zur Kontrolle. Weicht er vom Ablageort ab, am Ende erwähnen.
- **Ports eines Bricks:** blaue Kreise = Inports, orange = Outports, Beschriftung = Feldname.
- **Verbindungen:** `line`/`path` mit Pfeil, vom Outport zum Inport.
- **Gestrichelte graue Linie mit „offen“** = nicht verbundener Port, dafür keinen Code erzeugen.
- Fail/Error-Ports sind meist nicht dargestellt und werden nicht verbunden.

## Prüfungen vor dem Anlegen

### 1. Komponenten finden

Jeden Komponententyp suchen, und zwar nicht nur im eigenen Paket, sondern **im ganzen Workspace** (Nachbarmodule
neben dem Projekt, z. B. `grep -rn "^type <Typ> struct" --include=*.go ..`). Für jeden Fund prüfen, dass die
verwendeten Port-Felder existieren und welche Signatur `New(...)` hat.

### 2. Abhängigkeitsrichtung

Liegt ein Typ in einem anderen Modul, prüfen, ob das Projekt der SVG dieses Modul importieren darf:
- Hat das andere Modul ein `replace <modul> => ../<projekt>` auf das Projekt der SVG (z. B. Anwendung → natsFBP),
  hängt es selbst davon ab. Ein Import in die Gegenrichtung erzeugt einen Zyklus und baut nicht.
- Dann vorschlagen, das Netzwerk **in das Modul der Komponente zu verlegen**, und den Nutzer fragen. Wird verlegt,
  gilt dessen `CLAUDE.md`, und die SVG wird dorthin kopiert (siehe [SVG nachziehen](#svg-nachziehen)).

### 3. Fehlende Komponenten

Fehlt ein Typ, nichts erfinden, sondern abbrechen und dem Nutzer **konkrete Optionen** anbieten:
- die ähnlichste bestehende Komponente nennen (Ports, was sie erwartet), z. B. `AddComment` für `AddHelloComment`;
- **a)** neue Komponente auf deren Basis bauen (Logik wiederverwenden, nicht kopieren), mit Test und eigenem cmd;
- **b)** die bestehende Komponente verwenden, und sagen, was sich dadurch an Payload oder Verhalten ändert.

### 4. Namenskollision

Der Service-Name eines Bricks ist sein **kleingeschriebener Go-Typname**; er steht im Subject
(`fbp.<service>.ip.inport...`). Der kleingeschriebene Netzwerkname darf deshalb **keinem Knoten-Typ und keinem
anderen Brick im Projekt** entsprechen. Sonst teilen sich Netzwerk und Komponente den Inport-Endpoint in derselben
Queue-Group, Nachrichten werden zufällig verteilt, und das Netzwerk kann sich selbst aufrufen.
Beispiel: `addhellocomment.svg` → Netzwerk `Addhellocomment` kollidiert mit Komponente `AddHelloComment`.
Bei Kollision fragen: Komponente umbenennen (z. B. `HelloComment`) oder SVG umbenennen.

Alle offenen Fragen aus 2–4 möglichst in **einer** Rückfrage stellen.

## Go-Datei des Networks

Aufbau wie die Vorlage (`writerandom.go`):

- `type <Typ> struct { rte.Network; <Outport> rte.Outport ... }`, ein Feld pro Netz-Outport. Der Netz-Inport `Inport` kommt aus `rte.Network`.
- Kommentar über dem Typ: kurz beschreiben, was das Netzwerk tut, inkl. offener Ports (Sprache nach Projektkonvention, sonst deutsch).
- `New(systemDService string, natsCon *nats.Conn) *<Typ>`: `Version = "0.0.1"`, passende englische `Description`, Outports initialisieren, `ClassName`, `rte.Inititlise(this)`, `this.Network.Init()`.
- `StartService()` wie in der Vorlage.
- `Init()`: je Brick `new(<Komponente>).New("", nil)`; hat `New` **weitere Parameter** (z. B. `jiraClient`), dafür
  ebenfalls `nil` übergeben – der Knoten dient im Netzwerk nur der Namensermittlung. Dann `AddNode("<knotenname>", ...)`,
  danach die Verbindungen:
  - Netz-Inport → Brick: `this.ConInport(this.Inport, "<knoten>", <var>.<Inport>)`
  - Brick → Brick: `this.Con("<von>", <var>.<Outport>, "<nach>", <var>.<Inport>)`
  - Brick → Netz-Outport: `this.ConOutport(this.<Outport>, "<knoten>", <var>.<Outport>)`

## Neue Komponenten (nur nach Zustimmung, Option a)

- Ins passende Fachpaket legen (z. B. `pkg/jira/comment`), Muster der ähnlichsten Komponente übernehmen
  (Konstruktor, `Execute`, Fehler über `PushError`, Jira-Aufrufe mit `requestTimeout` in einer testbaren Hilfsmethode).
- Vorhandene Logik wiederverwenden statt duplizieren.
- Test im Stil des Projekts anlegen (z. B. `httptest.NewServer` + tabellengetrieben).
- Eigenen cmd-Ordner anlegen (siehe unten) und, falls das Projekt eine Strukturtabelle in der `CLAUDE.md` führt,
  dort ergänzen.

## cmd-Ordner

Ein Network startet die Services seiner Knoten nicht. Deshalb für **das Netzwerk und jede neu angelegte
Komponente** einen cmd-Ordner anlegen und für jeden bestehenden Knoten prüfen, dass es schon ein `cmd/.../main.go`
gibt (fehlt eines, am Ende darauf hinweisen).

1. `config.json`:
   - Netzwerk: wie die `config.json` der Vorlage (nur `natscon`).
   - Komponente: die `config.json` aus dem cmd der Komponente, deren Muster übernommen wurde (z. B. mit `jira`-Block).
     Per `cp` kopieren, ohne den Inhalt auszugeben, und am Ende erwähnen, dass darin ggf. ein Token mitkopiert wurde.
2. `main.go` als Kopie der passenden Vorlage-`main.go` (Netzwerk: eine ohne Zusatz-Config; Komponente: die der
   Musterkomponente), nur Import, Typ, Variablenname und Ausgaben („<Typ> is running...“, „<Typ> Nats Connection
   closed“) ändern. Danach per `diff` gegen die Vorlage kontrollieren.
3. Prüfen, testen und bauen, Go immer über `/snap/go/current/bin/go` (der Wrapper `/snap/bin/go` gibt nichts aus),
   aus der Projektwurzel:
   ```bash
   gofmt -l <geänderte ordner>
   /snap/go/current/bin/go vet ./<paketordner>/... ./cmd/<pfad>/<name>/
   /snap/go/current/bin/go test ./<paketordner>/...
   /snap/go/current/bin/go build -o cmd/<pfad>/<name>/<name> ./cmd/<pfad>/<name>/   # je Programm
   ```
   Fehler beheben, bis alles ohne Meldung durchläuft.

## SVG nachziehen

Wurde das Netzwerk verlegt oder wurden Komponenten umbenannt, muss die SVG zum Code passen:
- SVG ins Zielprojekt neben die Go-Datei **kopieren** (Original nicht löschen, am Ende anbieten).
- Knotennamen, Typnamen und Paketzeilen anpassen. Am zuverlässigsten mit dem Skill `rte-network-to-image` aus der
  fertigen Go-Datei. Bei `sed` nur gezielt ersetzen (Zeilennummer oder eindeutiger Kontext), denn Netzwerk- und
  Komponentenname können gleich lauten; danach die SVG als XML validieren und die geänderten Zeilen ansehen.

## Abschluss

- Die `.gitignore` nicht anfassen. `/cmd/*/*` mit `!/cmd/*/*.go` deckt Binary und config.json bei `cmd/<name>/` ab.
  Liegt der cmd-Ordner **eine Ebene tiefer** (`cmd/<gruppe>/<name>/`), erfasst `/cmd/*/*` den ganzen Ordner, und
  `!/cmd/*/*.go` holt `main.go` nicht zurück (Git schließt Dateien in ignorierten Ordnern nicht wieder ein). Das mit
  `git check-ignore -v cmd/<gruppe>/<name>/main.go` prüfen, falls ein Git-Repo da ist, und darauf hinweisen.
- Den Dienst nicht starten und nicht in die Controller-Config eintragen, aber am Ende anbieten – für **alle**
  Services, die für einen Durchlauf laufen müssen (Netzwerk und jeder Knoten).
- Kurz zusammenfassen: angelegte Dateien (als Links), Knoten und Verbindungen, offene Ports, Abweichungen vom Bild
  (Name, Ablageort, umbenannte Komponenten), Ergebnis von gofmt/vet/test/build.
