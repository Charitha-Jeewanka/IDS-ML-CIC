Dataset is delivered as **CSV files** organized by the **day of capture** (from Monday, July 3rd to Friday, July 7th).

The dataset structure corresponds to the daily attack schedule outlined in the papers:

*   **Monday:** Contains only **Benign** traffic.
*   **Tuesday:** Contains **Brute Force** attacks (specifically FTP and SSH).
*   **Wednesday:** Contains **DoS** attacks (Slowloris, Slowhttptest, Hulk, GoldenEye) and **Heartbleed**.
*   **Thursday:** Contains **Web Attacks** (Brute Force, XSS, SQL Injection) and **Infiltration** attacks.
*   **Friday:** Contains **DDoS** (LOIT), **Botnet** (ARES), and **PortScan** traffic.

The network flows in these CSV files were extracted and labelled using the **CICFlowMeter** software based on this daily schedule.