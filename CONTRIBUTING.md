## Build

    poetry install

## Running Tests

    poetry run pytest intake_hive

### Run Single Test

    poetry run pytest intake_hive -k search_item (e.g., name of test)
    
### Pre-commit
Run all pre commit checks (isort, black, flake8, pytest)

    poetry run pre-commit run --all-files

See https://docs.pytest.org/en/latest/usage.html for -k options