# Komoot: bundling notifications app

First, install the required packages (I am using python 3.6.8)
```
$ pip install -r requirements.txt
```

Then run:

```
$ python notifications.py --help
```

to get help on options.

Run:
```
$ python notifications.py <input_path> <output.csv>
```
to get bundles for **exact** method (see provided notebook). For **predictions** method, run
```
$ python notifications.py <input_path> <output.csv> --typ=predict
```
> **Note**: this app could use more error handling, but it is what it is and does it's job.