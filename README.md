# Livedocs SDK

A Python SDK that enables Livedocs to run helper functions for various elements, such as charts, tables, SQL queries, and text.

---

## Requirements

1. **Install `uv` (latest version)**
   Download and install from the official documentation:
   [https://docs.astral.sh/uv/getting-started/installation/](https://docs.astral.sh/uv/getting-started/installation/)

2. **Install Python** (version **3.12**)
   Download and install from:
   [https://www.python.org/downloads/](https://www.python.org/downloads/)

---

## Installation

Before installing the SDK, ensure your Python environment is set up. `uv` handles dependency resolution and installation, so no separate `pip`, `setuptools`, or `wheel` setup is required.

### Install the SDK in Development Mode

If you’ll be actively working on the SDK, install it in *editable mode* so your environment always reflects the latest local changes.

Make sure this command is run in the same virtual environment used by Middleman:

```bash
uv pip install -e .
```

### Install the SDK as a Standalone Package

To install the SDK as a standalone dependency directly from GitHub:

```bash
uv pip install git+https://github.com/livedocs-io/sdk.git
```

---

## Testing

To test chart functions directly from the client:

1. **Install testing dependencies**:

   ```bash
   uv pip install ".[test]"
   ```

2. **Run tests**:

   ```bash
   CORE_BASE_URL=http://localhost:4000 flask --app tests.vega-api run
   ```
* Tighten wording for external/open-source audiences
