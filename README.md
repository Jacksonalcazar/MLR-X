# MLR-X

MLR-X is a **multiple linear regression (MLR)** application with both a desktop graphical user interface (GUI, based on Tkinter) and a command-line interface (CLI). It is designed to operate efficiently on both low- and high-dimensional datasets.

## Features

- Desktop GUI for configuring and running multiple linear regression analyses.
- CLI support for automated and reproducible workflows.
- Export of results and visualizations in multiple formats.
- Integration with Python scientific libraries (NumPy, pandas, statsmodels, Pillow, scikit-learn, matplotlib).

## Requirements

- Python 3.10 or higher (recommended).

On Linux, install the required system packages before running:

```bash
sudo apt-get install python3-tk
sudo apt-get install xvfb
```

## Installation and Run Options

Choose **one** of the following methods to use MLR-X:

### Option A) Install from PyPI (recommended)

```bash
pip install mlr-x
```

Run the application:

```bash
mlrx
```

> In this mode, all required dependencies are installed automatically via `pip`.

### Option B) Run from source

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the application:

```bash
python MLRX.py
```

## Usage

### GUI mode

- Installed package: `mlrx`
- Source mode: `python MLRX.py`

When executed without arguments, MLR-X launches the graphical user interface.

### CLI mode

Installed package:

```bash
mlrx <config.conf> [--model <id>] [--outputs ...] [--noruns]
```

Source mode:

```bash
python MLRX.py <config.conf> [--model <id>] [--outputs ...] [--noruns]
```

#### Key parameters

- `--version`: Display version information and exit.
- `--model`: Select a model identifier for output generation.
- `--outputs`: Specify outputs to generate (e.g., `diagnostics`, `visualization`, `summary`).
- Visualization outputs support formats such as `pdf`, `png`, `tiff`, and `svg`.
- `--noruns`: Use an existing results file from the configured output path (skips model execution).
- `--onlyIV` / `--onlyEV`: Perform only internal or external validation, respectively, using an existing results file. These options skip model search and require precomputed results.

#### Example

```bash
python MLRX.py example.conf --model 1 --outputs summary
```

## Portable Binaries

Portable executables are provided for end users who prefer not to install Python or manage dependencies manually.

Download available builds from:  
https://jacksonalcazar.github.io/MLR-X

### Supported platforms

- Windows 10/11 (64-bit)
- macOS (Arm64)
- Ubuntu 20.04 (x86-64)

## Documentation

- Software manual (included in this repository): `MLR_X_v_1_0_Manual.pdf`
- Precompiled binaries and releases: https://github.com/Jacksonalcazar/MLR-X/releases
- Issue tracker: https://github.com/Jacksonalcazar/MLR-X/issues

## License

This project is distributed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. See `LICENSE.txt` for details.

## Trademark

The MLR-X name, logo, and visual identity are subject to trademark terms. See `TRADEMARK.md` for usage guidelines.
