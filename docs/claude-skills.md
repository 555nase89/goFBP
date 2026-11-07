# Claude skills

The repository ships [Claude Code](https://claude.com/claude-code) skills in `.claude/skills/` that
create Networks from text or images and draw Networks as SVG. Claude Code loads them when it is
started in this repository. To use them in other projects, link them into `~/.claude/skills/`:

```sh
for s in fbp-network-from-text fbp-network-from-image fbp-network-to-image; do
    ln -s "$PWD/.claude/skills/$s" ~/.claude/skills/$s
done
```

The skill instructions are written in German.

## goFBP Networks

| Skill | Input | Result |
|-------|-------|--------|
| `/fbp-network-from-text <name>.fbp` | arrow notation (below) | `<name>_net.go` with test, `cmd/<name>/` for Services, `<name>.svg` |
| `/fbp-network-from-image <name>.svg` | SVG of a Network | `<name>_net.go` with test, `cmd/<name>/` for Services |
| `/fbp-network-to-image <name>_net.go` | Network constructor | `<name>.svg` next to the Go file, or an existing SVG updated to the code |

The name of the Network always comes from the file name. Before writing any file, the
`from-*` skills check that every Brick exists, that imports create no cycle and that the Network
name is unique in its package. Open questions are asked all at once. Existing files are not
overwritten without asking.

`fbp-network-from-text` is the quickest way: a few lines of text instead of a drawing, and the
SVG is drawn for you.

### Arrow notation

`pkg/record/record.fbp` for the Network `record`:

```
description: writes Events and log records of goFBP Services to the store
storeevent = StoreEvent(st)

IN  > checkevent > storeevent > Success
Log > checklog   > storelog   > Success
*.Error > Error
```

| Notation | Meaning | Code |
|----------|---------|------|
| one line | a chain; `>` connects each element to the next | |
| `checkevent` (lower case) | Node | `n.Add("checkevent", CheckEvent())` |
| `IN`, `Log`, `Success`, `Rejected` (upper case first) | Network Inport or Outport | |
| `a > b` | Outport `Success` of `a` to `b` | `n.Connect("a", fbp.Success, "b")` |
| `a.Invalid > b` | Outport `Invalid` of `a` to `b` | `n.Connect("a", "Invalid", "b")` |
| `a.Out > Other:nested` | to Inport `Other` of a nested Network | `n.ConnectTo("a", "Out", "nested", "Other")` |
| `IN > a` | default Inport to `a` | `n.ConnectIn("a")` |
| `Log > a` | additional Inport `Log` to `a` | `Inports: []string{"Log"}`, `n.ConnectInport("Log", "a")` |
| `a > Success`, `a.Error > Error` | to a Network Outport | `n.ConnectOut("a", fbp.Success, fbp.Success)` |
| `a.Invalid > Rejected` | to an additional Network Outport | `Outports: []string{"Rejected"}`, `n.ConnectOut(...)` |
| `*.Error > Error` | `Error` of every Node to the Network's `Error` | one `ConnectOut` per Node |
| `IN > a` and `IN > b` | Fan-out | |
| `description: ...` | `Network.Description` | |
| `node = Constructor(args)` | constructor of a Node; without this line the one whose lower-case name equals the node name | |

Empty lines and lines starting with `#` are ignored. The `.fbp` file is versioned next to the
`_net.go` and is the source of the Network: change the file and run the skill again; it shows the
differences to the existing code before changing it.

## natsFBP Networks

For `rte.Network` from natsFBP, not goFBP: `rte-network-from-image`, `rte-network-to-image` and
`rt-network-draw-to-svg` (photo or scan of a sketch to SVG).
