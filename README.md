# Test for the session functionality

## Installation

1. To install all the dependencies execute
```bash
pipenv install --dev
```

2. Create a Pub/Sub topic

3. Create a .env file with the following content:

    GOOGLE_CLOUD_PROJECT=your_project
    topic=projects/PROJECT/topics/TOPIC



## Execution

To execute the pipeline run

```bash
pipenv shell
```

```bash
python -m pipeline.pipeline \
--runner DirectRunner \
--input_topic $topic
```

Once the pipeline is runnung execute the "publish_messages.py"