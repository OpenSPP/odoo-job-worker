# Contributing

## Development Setup

1. Clone the repository:

    ```bash
    git clone https://github.com/OpenSPP/odoo-job-worker.git
    cd odoo-job-worker
    ```

2. Place the modules in your Odoo 19.0 addons path (or symlink them).

3. Install the modules in a development database:

    ```bash
    odoo -d dev_db \
      --addons-path=/path/to/odoo/addons,/path/to/odoo-job-worker \
      -i job_worker,job_worker_monitor,job_worker_demo \
      --stop-after-init
    ```

## Running Tests

### With Docker (recommended)

The repository includes a Docker-based test runner that handles PostgreSQL setup:

```bash
bash docker/run_tests.sh
```

To test a specific module:

```bash
bash docker/run_tests.sh job_worker
bash docker/run_tests.sh job_worker_monitor
```

The script:

1. Starts a PostgreSQL 16 container
2. Creates a test database per module
3. Runs `odoo --test-enable --test-tags /<module> --stop-after-init`
4. Reports pass/fail based on test output

### Without Docker

Run tests directly with an Odoo instance:

```bash
odoo -d test_db \
  --addons-path=/path/to/odoo/addons,/path/to/odoo-job-worker \
  --test-enable \
  --test-tags /job_worker \
  --stop-after-init \
  -i job_worker
```

## Code Style

### Python

Follow PEP 8 and the Odoo coding guidelines. The project uses:

- **ruff** for linting and formatting

Run checks on changed files:

```bash
pre-commit run ruff --files <changed_files>
pre-commit run ruff-format --files <changed_files>
```

### XML / JavaScript

Format with prettier:

```bash
pre-commit run prettier --files <changed_files>
```

## Pre-Commit Hooks

Install pre-commit hooks for automatic checks:

```bash
pip install pre-commit
pre-commit install
```

Hooks run automatically on `git commit`.

## Documentation

The documentation site is built with MkDocs Material. To preview locally:

```bash
uv sync --dev
uv run mkdocs serve
```

Then open [http://localhost:8000](http://localhost:8000).

Documentation source files are in the `docs/` directory. The site is
automatically built and deployed to GitHub Pages on push to `main`.

## Submitting Changes

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes with tests
4. Run the test suite to verify
5. Submit a pull request

## Bug Reports

File bugs on [GitHub Issues](https://github.com/OpenSPP/odoo-job-worker/issues).
Include:

- Odoo version and PostgreSQL version
- Steps to reproduce
- Expected vs. actual behavior
- Relevant log output or tracebacks

## License

This project is licensed under [LGPL-3.0](https://github.com/OpenSPP/odoo-job-worker/blob/main/LICENSE).
