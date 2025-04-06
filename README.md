# sistemas-distribuidos-1

## Grupo N

| Nombre                     | Legajo | Email              |
| -------------------------- | ------ | ------------------ |
| Gómez Belis, Sofía         | 109358 | sgomezb@fi.uba.ar  |
| Jáuregui, Melina Belén     | 109524 | mjauregui@fiuba.ar |
| Tourne Passarino, Patricio | 108725 | ptourne@fi.uba.ar  |

## Development setup

### Local go instalation

Use go v1.23.7

### Nix setup

#### Install Nix

Go to [Nix.org](https://nixos.org/download/)

#### Activate flakes

Run

```bash
echo "experimental-features = nix-command flakes" > ~/.config/nix/nix.conf
```

or edit `~/.config/nix/nix.conf` manually and add the following line:

```
experimental-features = nix-command flakes
```

#### Usage

Open the dev shell with:

```bash
nix develop
```

Or

```bash
nix develop ./path_to_project
```

To open your editor with nix:

**Visual Studio Code**:

```bash
nix develop ./path_to_project -c code ./path_to_project
```

**Zed**:

```bash
nix develop ./path_to_project -c zed ./path_to_project
```
