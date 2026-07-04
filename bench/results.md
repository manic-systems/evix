| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `evix local=1` | 347.8 ± 9.1 | 339.7 | 363.0 | 6.99 ± 0.27 |
| `evix local=4` | 248.4 ± 10.2 | 236.1 | 260.9 | 5.00 ± 0.25 |
| `evix local=8` | 294.2 ± 11.6 | 283.9 | 310.9 | 5.91 ± 0.29 |
| `evix distributed remote=4` | 306.6 ± 6.8 | 298.6 | 317.4 | 6.16 ± 0.23 |
| `evix distributed local=4 remote=4` | 286.6 ± 11.0 | 275.9 | 302.3 | 5.76 ± 0.28 |
| `evix daemon prewarm local=4` | 396.4 ± 6.4 | 389.6 | 403.7 | 7.97 ± 0.27 |
| `evix daemon warm query full local=4` | 188.6 ± 3.7 | 183.7 | 192.5 | 3.79 ± 0.13 |
| `evix daemon warm query n0 local=4` | 49.7 ± 1.5 | 48.2 | 52.0 | 1.00 |
| `nix-eval-jobs w=4` | 238.7 ± 6.3 | 228.7 | 246.1 | 4.80 ± 0.19 |
