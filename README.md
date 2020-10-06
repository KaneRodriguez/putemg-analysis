# putemg-analysis

### Install Dependencies

Install the [poetry](https://github.com/python-poetry/poetry) package manager with:
```bash
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3
```
Add $HOME/.poetry/bin to your $PATH:
```bash
export PATH=$HOME/.poetry/bin:$PATH
```
Install the project dependencies:
```bash
poetry install
```

### Install Precommit

Enter a poetry shell:
```bash
poetry shell
```

Install [pre-commit](https://pre-commit.com/) hooks:
```bash
pre-commit install
```

### Local Development

To run the jupyter notebook locally, run the following commands:

1. Enter a poetry shell
    ```bash
    poetry shell
    ```
2. Start jupyter
    ```shell
    jupyter notebook
    ```
3. Navigate to and open the `data-analysis.ipynb` notebook
