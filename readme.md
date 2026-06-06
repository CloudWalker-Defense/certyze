# Microsoft Learn Cert → PDF Builder

A typical Microsoft Learn cert is 8-12 modules of 5-8 unit pages each. That's 60+ separate browser tabs end-to-end. The curriculum is published as a sequence of in-browser modules, with no offline export and no consolidated search.

This tool generates PDFs you can read offline, search across, or upload to NotebookLM, Claude, ChatGPT, or any other LLM for exam prep.

The repository contains two components:

1. **The PDFs.** Pass any Microsoft Learn cert URL and generate one PDF per module and a combined PDF per learning path.
2. **The workflow.** A prompt sequence covering gap analysis, exam-style quizzes, Feynman drills, and a day-by-day schedule. Full walkthrough and pre-built ZIPs at [cloudwalkerdefense.com/certyze](https://www.cloudwalkerdefense.com/certyze).

---

## Prerequisites

- **Python 3.11+**
- Python packages: see [`requirements.txt`](requirements.txt)

---

## Installation

### Clone

```bash
git clone https://github.com/CloudWalker-Defense/certyze
cd mslearn-cert-prep
```

### Set up a virtual environment

**Windows (PowerShell):**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

> If activation fails with an execution policy error, run the following command once to allow signed scripts: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

> **Linux**: if `playwright install` fails or Chromium fails to launch, run `playwright install-deps` first to install required system libraries.

---

## Usage

### Find a certification URL

Browse all Microsoft certifications at:
**https://learn.microsoft.com/en-us/credentials/browse/?credential_types=certification**

Copy any cert URL: `https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/`

### Run

Pass the cert URL as an argument:

```bash
python main.py "https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/"
```

Run with no arguments to be prompted instead:

```bash
python main.py
```

---

## Output layout

Output structure when run against DP-700 (`fabric-data-engineer-associate`):

```
output/
└── DP-700-fabric-data-engineer-associate/
    ├── DP700-D1-Ingest data with Microsoft Fabric.pdf
    ├── DP700-D2-Implement a Lakehouse with Microsoft Fabric.pdf
    ├── DP700-D3-Implement Real-Time Intelligence with Microsoft Fabric.pdf
    ├── DP700-D4-Implement a data warehouse with Microsoft Fabric.pdf
    ├── DP700-D5-Manage a Microsoft Fabric environment.pdf
    ├── D1-Ingest data with Microsoft Fabric/
    │   ├── D1M1-Ingest Data with Dataflows Gen2 in Microsoft Fabric.pdf
    │   ├── D1M2-Orchestrate processes and data movement with Microsoft Fabri.pdf
    │   ├── D1M3-Use Apache Spark in Microsoft Fabric.pdf
    │   └── D1M4-Work with real-time data in an Eventhouse in Microsoft Fabri.pdf
    ├── D2-Implement a Lakehouse with Microsoft Fabric/        (7 modules)
    ├── D3-Implement Real-Time Intelligence with Microsoft Fabric/  (5 modules)
    ├── D4-Implement a data warehouse with Microsoft Fabric/   (6 modules)
    └── D5-Manage a Microsoft Fabric environment/              (4 modules)
```

The PDFs at the cert root are the combined learning-path PDFs, suitable for end-to-end reading, search, and LLM upload. The D{N}-.../ folders contain individual module PDFs.

---

## How it works

1. Loads the certification page and each learning-path page to extract module URLs.
2. Fetches each unit page in the module sequence.
3. Cleans the HTML, preserves diagrams and images, and renders each module to PDF.
4. Stitches module PDFs into combined learning-path PDFs.

---

## Configuration

Configuration constants (rate limit, concurrency, timeouts) are defined at the top of [`main.py`](main.py).

---

## Support

Questions, bugs, or feedback? Reach out to us at [info@cloudwalkerdefense.com](mailto:info@cloudwalkerdefense.com).

---

## License

[MIT License](LICENSE)
