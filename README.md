# Running

## Dependencies

You'll need to install the dependencies first, which are managed in the `pyproject.toml` file.  I'll assume a virtual environment; eg

[UV](https://docs.astral.sh/uv/) (recommended, super fast and can also manage python versions):
```sh
uv sync .
```

Pip (almost certainly already installed)
```sh
pip install -e .
```

Anaconda... you're on your own.


## Running the dev server

(after activating the environment, so that `dagster` is in your path):

```sh
dagster dev -f workflow.py
```

And open [](http://localhost:3000).
