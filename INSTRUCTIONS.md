# Instructions to Run Challenges

1. Install dependencies

```sh
pip install -r requirements.txt
```

2. Add credentials file to `.secrets/credentials` at the root of repository.

```ini
[default]
aws_access_key_id=ADD_ACCESS_KEY_ID_HERE
aws_secret_access_key=ADD_SECRET_ACCESS_KEY_HERE
region_name=af-south-1
```

3. Run the following script to get access to CLI

```
python challenges.py --help
```
