| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `evix local=1` | 676.3 ± 12.0 | 667.7 | 694.4 | 1.81 ± 0.06 |
| `evix local=4` | 376.3 ± 7.5 | 368.1 | 388.6 | 1.01 ± 0.03 |
| `evix local=8` | 373.0 ± 5.5 | 366.2 | 380.4 | 1.00 ± 0.03 |
| `evix distributed remote=4` | 496.8 ± 5.7 | 489.7 | 503.0 | 1.33 ± 0.04 |
| `evix distributed local=4 remote=4` | 410.3 ± 4.7 | 403.1 | 415.9 | 1.10 ± 0.03 |
| `evix daemon prewarm local=4` | 683.9 ± 15.2 | 665.7 | 700.8 | 1.83 ± 0.06 |
| `evix daemon warm query local=4` | 392.0 ± 5.0 | 385.5 | 397.6 | 1.05 ± 0.03 |
| `nix-eval-jobs w=4` | 372.7 ± 9.7 | 356.9 | 380.5 | 1.00 |
