# Laissez-faire

Upbit / Bithumb automated trading bot.

## Layout

```
laissez-faire/
  run.py
  key.txt
  log/command.txt
  pyproject.toml
  src/
    __main__.py
    engine.py
    parallel.py
    command_sync.py
    paths.py
    config.py
    state.py
    ...
```

## Run

```bash
python run.py -e upbit
python run.py -e upbit --no-command-sync
```

After `pip install -e .`:

```bash
python -m laissez_faire -e upbit
```

`command_sync` starts in a background thread and updates `log/command.txt` from pastebin.
