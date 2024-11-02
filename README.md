# vm-lib

A Python SDK that enables Livedocs to run helper functions for various elements, such as charts, tables, SQL queries, and text.

---

## Requirements

1. **Install Python** (version 3.12).  
   - Download and install: [Python Downloads](https://www.python.org/downloads/).

---

## Installation

Before installation, ensure your build system is set up with `setuptools`, `wheel`, and other build dependencies. If these are not already installed, you can do so by running:

```bash
pip install --upgrade pip setuptools wheel
```

### Install vm-lib in Development Mode

If you’ll be actively working with `vm-lib`, install it in *editable mode* so the kernel points to your latest changes. Make sure this command is run in the same environment where Middleman’s `VIRTUAL_ENV` is poined to:

```bash
pip install -e .
```

### Install vm-lib as a Standalone Package

To install `vm-lib` as a standalone library, use the following command, replacing `username` and `password` with your GitHub credentials:

```bash
pip install git+https://username:password@github.com/livedocs-io/vm-lib.git
```

---

## Testing

To test chart functions directly from the client, run:

1. **Install Testing Dependencies**:

    ```bash
    pip install ".[test]"
    ```

2. **Run Tests**:

    ```bash
    CORE_BASE_URL=http://localhost:4000 flask --app tests.vega-api run
    ```