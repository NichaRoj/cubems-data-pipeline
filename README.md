# cubems-data-pipeline

Data Pipeline for Data Analysis built separately from CUBEMS environment

## Running Dataflow Pipeline

1. Go into dataflow folder

2. Set up virtual environment

```
python3 -m venv venv
```

3. Activate the virtual environment

- Windows: `venv\Scripts\activate.bat`
- Linux/Mac: `source venv/bin/activate`

4. Install required packages

```
pip3 install -r requirements.txt
```

## Deploy Dataflow Pipeline

1. Setup `GOOGLE_APPLICATION_CREDENTIAL` environment variable

```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

OR create `.env` with the following content

```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

2. Run pipeline locally.

```
python3 direct-runner.py
```

3. Deploying FDataflow Template. The filename must be the same as template name and the file must follow the guideline https://cloud.google.com/dataflow/docs/guides/templates/creating-templates

```
python3 deploy.py --template [template name]
```

## Deploying Firebase Functions

1. Go into functions folder

2. Install dependencies

```
yarn
```

3. Deploy to Firebase

```
yarn deploy
```
