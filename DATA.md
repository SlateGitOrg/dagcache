# Data

No dataset is committed to this repository and nothing is downloaded.

| data | used by | source | license | how it is obtained |
|---|---|---|---|---|
| UCI Wine recognition data (178 samples, 13 features, 3 cultivars) | `examples/wine` (the `load` stage), `tests/test_example_wine.py`, `make demo` | Aeberhard, S. & Forina, M. (1991). *Wine* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5PC7J | CC BY 4.0 (UCI); redistributed inside scikit-learn (BSD-3-Clause) | `sklearn.datasets.load_wine()` reads the copy bundled with the installed scikit-learn 1.9.1 (pinned in `uv.lock`); offline |
| Seeded pseudo-random samples | `examples/decorators` | generated (`random.Random(seed)`) | n/a | synthetic |
| Random bytes, synthetic 300-stage DAG, sleep stages | `bench/bench.py` | generated (`os.urandom`) at run time in a temp dir, deleted afterwards | n/a | synthetic |
| Small text fixtures | `tests/` | written by the tests into pytest temp dirs | n/a | synthetic |

Evaluation hygiene in the example: `split` makes a stratified 70/30 split with a
fixed seed. `featurize` fits its scaler on the train split only. `evaluate` and
`baseline` are the only stages that read the test split. Nothing is tuned on the
test split. The demo changes `train.C` only to show caching, not to pick a
model.
