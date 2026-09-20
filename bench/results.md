| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `evix local=1` | 263.3 ± 6.5 | 258.1 | 274.6 | 9.31 ± 0.32 |
| `evix local=4` | 157.4 ± 4.5 | 151.0 | 163.2 | 5.57 ± 0.21 |
| `evix local=8` | 153.2 ± 2.4 | 151.0 | 156.9 | 5.42 ± 0.16 |
| `evix distributed remote=4` | 198.3 ± 2.9 | 194.6 | 201.1 | 7.02 ± 0.20 |
| `evix distributed local=4 remote=4` | 168.4 ± 11.1 | 156.9 | 186.7 | 5.96 ± 0.42 |
| `evix daemon prewarm local=4` | 197.0 ± 6.5 | 186.5 | 203.1 | 6.97 ± 0.29 |
| `evix daemon warm query full local=4` | 103.3 ± 0.7 | 102.4 | 104.2 | 3.65 ± 0.09 |
| `evix daemon warm query n0 local=4` | 28.3 ± 0.7 | 27.6 | 29.4 | 1.00 |
| `nix-eval-jobs w=4` | 147.5 ± 7.6 | 135.4 | 154.3 | 5.22 ± 0.30 |
