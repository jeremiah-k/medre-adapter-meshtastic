# medre-adapter-meshtastic

Independent meshtastic adapter for MEDRE, extracted from MEDRE commit
`e59c29dd31a95860d1cdbc19e531896616627a68`.

This repository retains the original adapter source history, with original
Git author and committer identity metadata. The import path stays
`medre.adapters.meshtastic`; the distribution registers an entry point in
`medre.adapters`, so MEDRE can discover it without a built-in adapter list.

Installation sources (MEDRE and this adapter must share one Python environment):

```bash
# Package index, if/when this distribution is published:
python -m pip install 'medre-adapter-meshtastic'

# Git source; replace FULL_COMMIT_SHA with a reviewed 40-character commit ID:
python -m pip install 'medre-adapter-meshtastic @ git+https://github.com/jeremiah-k/medre-adapter-meshtastic.git@FULL_COMMIT_SHA'

# Local development checkout:
python -m pip install -e ../medre-adapter-meshtastic
```

Git installation uses standard Python direct URL requirements, not a custom
MEDRE downloader. To install before MEDRE itself is published, include MEDRE's
Git URL as a dependency source in the same deployment environment. For uv,
use `tool.uv.sources` with an exact Git `rev` or local `path`; commit the
resulting `uv.lock` for reproducibility. Installing an adapter does not require
publishing it on PyPI. Runtime startup never installs or updates dependencies.

After installation/restart, use `medre adapters check --config bridge.yaml` to
verify that the transport is discoverable. See
[MEDRE adapter installation](https://github.com/jeremiah-k/medre/blob/main/docs/ops/adapter-installation.md)
for mixed PyPI/Git/local installations and community adapters.

License: GPL-3.0-or-later for the original MEDRE-authored adapter files.
Upstream SDK licenses remain separate and must be reviewed for redistribution.
The LXMF/RNS integration in particular does **not** gain license insulation
merely by being moved to another repository.
