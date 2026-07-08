| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `evix local=1` | 317.8 ± 8.0 | 312.3 | 331.9 | 3.21 ± 0.12 |
| `evix local=4` | 197.6 ± 5.7 | 188.3 | 202.3 | 2.00 ± 0.08 |
| `evix local=8` | 216.3 ± 11.7 | 200.8 | 232.0 | 2.19 ± 0.13 |
| `evix distributed remote=4` | 268.7 ± 12.1 | 258.9 | 286.7 | 2.71 ± 0.14 |
| `evix distributed local=4 remote=4` | 242.2 ± 7.1 | 229.8 | 247.0 | 2.45 ± 0.10 |
| `evix daemon prewarm local=4` | 313.1 ± 10.8 | 295.6 | 324.6 | 3.16 ± 0.14 |
| `evix daemon warm query full local=4` | 344.8 ± 96.9 | 172.2 | 402.0 | 3.48 ± 0.98 |
| `evix daemon warm query n0 local=4` | 99.0 ± 2.8 | 96.4 | 102.1 | 1.00 |
| `nix-eval-jobs w=4` | 358.9 ± 7.1 | 351.1 | 367.0 | 3.63 ± 0.13 |
