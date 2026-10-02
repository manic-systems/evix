| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `evix local=1` | 840.0 ± 17.0 | 813.4 | 855.5 | 21.22 ± 0.52 |
| `evix local=4` | 424.4 ± 17.4 | 397.4 | 439.6 | 10.72 ± 0.46 |
| `evix local=8` | 437.7 ± 22.9 | 419.9 | 477.5 | 11.06 ± 0.60 |
| `evix distributed remote=4` | 571.2 ± 23.9 | 543.0 | 599.9 | 14.43 ± 0.64 |
| `evix distributed local=4 remote=4` | 446.7 ± 24.4 | 418.2 | 483.8 | 11.28 ± 0.64 |
| `evix daemon prewarm local=4` | 532.2 ± 6.5 | 525.5 | 542.8 | 13.44 ± 0.25 |
| `evix daemon warm replay local=4` | 39.6 ± 0.6 | 39.0 | 40.5 | 1.00 |
| `evix daemon warm query full local=4` | 286.3 ± 2.4 | 283.6 | 289.3 | 7.23 ± 0.12 |
| `nix-eval-jobs w=4` | 438.6 ± 4.3 | 435.3 | 446.0 | 11.08 ± 0.19 |
