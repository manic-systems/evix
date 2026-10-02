| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `evix local=1` | 118.8 ± 4.6 | 112.5 | 124.5 | 11.75 ± 0.81 |
| `evix local=4` | 62.1 ± 2.7 | 60.3 | 66.9 | 6.14 ± 0.44 |
| `evix local=8` | 58.1 ± 2.9 | 54.4 | 61.6 | 5.75 ± 0.43 |
| `evix distributed remote=4` | 82.3 ± 5.4 | 76.2 | 91.1 | 8.14 ± 0.71 |
| `evix distributed local=4 remote=4` | 61.8 ± 2.7 | 59.7 | 66.1 | 6.12 ± 0.44 |
| `evix daemon prewarm local=4` | 64.2 ± 1.2 | 62.8 | 65.4 | 6.35 ± 0.38 |
| `evix daemon warm replay local=4` | 12.9 ± 0.7 | 11.9 | 13.6 | 1.28 ± 0.10 |
| `evix daemon warm query full local=4` | 13.5 ± 0.3 | 13.2 | 14.0 | 1.34 ± 0.08 |
| `evix daemon warm query n0 local=4` | 10.1 ± 0.6 | 9.5 | 10.7 | 1.00 |
| `nix-eval-jobs w=4` | 80.5 ± 7.3 | 75.2 | 92.0 | 7.96 ± 0.85 |
