# medre-adapter-meshtastic

Independent meshtastic adapter for MEDRE, extracted from MEDRE commit
`e59c29dd31a95860d1cdbc19e531896616627a68`.

This repository retains the original adapter source history, with original
Git author and committer identity metadata. The import path stays
`medre.adapters.meshtastic`; the distribution registers an entry point in
`medre.adapters`, so MEDRE can discover it without a built-in adapter list.

Installation (after MEDRE is published):

```bash
pip install medre-adapter-meshtastic
```

For local development, install MEDRE and this repo in editable mode into
one environment. MEDRE provides core configuration, contracts, and shared
fakes. Runtime initialization does not automatically download packages.

License: GPL-3.0-or-later for the original MEDRE-authored adapter files.
Upstream SDK licenses remain separate and must be reviewed for redistribution.
The LXMF/RNS integration in particular does **not** gain license insulation
merely by being moved to another repository.
