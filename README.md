# `vm-lib`

A Python SDK that allows Livedocs notebooks to run helper functions for various elements, such as charts, tables, SQL queries, and text.

## Requirements

- **Python 3.12 or higher**  
   Make sure you have Python 3.12+ installed

## Installation

```bash
pip install git+https://github.com/livedocs-io/vm-lib.git
```

If you run into any repository permission issues, you can try using this command (replace `username` and `password` with your GitHub credentials):

```bash
pip install git+https://username:password@github.com/livedocs-io/vm-lib.git
```

### **Important Note:**  
Before running the installation, make sure your build system is properly set up. You should have `setuptools`, `wheel`, and other build dependencies installed. If you don't have them installed, you can run:

```bash
pip install --upgrade pip setuptools wheel
```

## Testing

To run tests for the project, use the following command:

```bash
python livedocs/test.py
```