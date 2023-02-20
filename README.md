
# Устновка

> Перед началом установки, надо установить `pipenv`: https://pipenv.pypa.io/en/latest/install/

```bash
$ pipenv install
$ pipenv shell
$ pip install .
```

# Примеры использования
# Export data
```
python -m solrdumper http://10.113.18.48:8983 -c askid export .
```

# Import data
```
python -m solrdumper http://10.113.18.48:8983 -c askid import <filename>
```

# Export configs

```
python -m solrdumper http://10.113.18.48:8983 export-configs .\configs\krea
```

# Import config
```
python -m solrdumper -u "username" -p "password" https://31.135.8.105:9113 import-config .\configs\krea\askid\
```
