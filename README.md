# Introduction to GenAI with Apache Airflow®

Welcome! :rocket:

This repository contains the code to be used in the Introduction to GenAI with [Apache Airflow®](https://airflow.apache.org/) [Astronomer Academy module](https://academy.astronomer.io/introduction-to-genai-with-apache-airflow).

## Academy Module Contents

In the Introduction to GenAI with Airflow module, you will learn everything you need to create your first retrieval augmented generation (RAG) application with Airflow. 

We will begin by introducing Generative AI (GenAI): what it is and how Airflow helps power GenAI applications. Next, we will cover the module project, a content generation application using a [Streamlit](https://streamlit.io/) frontend that creates custom text based on local data. The application's data will be made available using an Airflow pipeline consisting of one DAG that ingests local files, chunks the text using [LangChain](https://www.langchain.com/), embeds it using [OpenAI](https://openai.com/), and stores it into a [Weaviate](https://weaviate.io/) vector database.

After covering the basics and explaining the Airflow DAG powering the RAG application, you will clone a GitHub repository with a pre-built Airflow environment in which we will build the DAG step-by-step. While building the DAG you will learn about key Airflow features such as the TaskFlow API, advanced DAG parameters, Airflow branching and dynamic task mapping.

Finally, we conclude by adapting the application for your personal use case and explore a real-world application similar RAG DAGs: [AskAstro](https://ask.astronomer.io/); a chat-bot with advanced knowledge about Airflow and Astronomer.

> [!NOTE]
> The fully finished DAG can be found in the `solutions` folder, if you are in a hurry!

## How to use this repository

1. :star: the repository and clone it to your local machine.
2. Create a new file at the root of the project called `.env` and copy the contents from `.env_example` into it.
3. Provide your own OpenAI API key in the `.env` file in the two places with the `<YOUR-OPENAI-API-KEY>` placeholders.
4. Make sure you have the open-source and free [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed on your local machine, and that it is up to date: this project runs Airflow 3, which needs a recent version of the CLI.
5. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 7 Docker containers on your machine, 5 for different Airflow components and one each for Weaviate and Streamlit:

- Postgres: Airflow's Metadata Database (port: 5432)
- API server: The Airflow component responsible for serving the Airflow UI and the Airflow API (port: 8080)
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG processor: The Airflow component responsible for parsing your DAG files
- Triggerer: The Airflow component responsible for triggering deferred tasks
- Weaviate: A local Weaviate instance (port: 8081 and 50051)
- Streamlit: A local Streamlit application (port: 8501)

6. Verify that all 7 Docker containers were created by running 'docker ps'. The streamlit container can take a few minutes to fully start.

> [!NOTE] 
> Your project uses the ports 8080, 8081, 5432, 50051 and 8501. If 8080 or 5432 are already taken, recent versions of the Astro CLI pick free ports for Airflow automatically and serve the UI through a local proxy instead - just use the URL that 'astro dev start' prints. The ports 8081, 50051 and 8501 are not remapped for you: if those are in use, either stop whatever is holding them or change them for Weaviate and Streamlit in the `docker-compose.override.yml` file. See the [Astronomer documentation for how to change the Astro project related ports](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#change-the-default-port-assignment).

7. Access the Airflow UI for your local Airflow project. To do so, go to the URL that 'astro dev start' printed, which is http://localhost:8080/ unless that port was already in use. No login is required: Airflow 3 runs locally with authentication disabled, so the UI opens straight away.

> [!NOTE]
> Because authentication is off, Airflow signs you in as a built-in admin user called `Anonymous`, and the "Log out" button simply logs you straight back in. Neither the user name nor that behaviour can be changed while authentication is off. If you would rather have a real login screen, with 'admin' as both Username and Password, uncomment the `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` setting in the `docker-compose.override.yml` file and run 'astro dev restart'.

8. Follow the Academy module to build your RAG DAG.

## Dependency management with [uv](https://docs.astral.sh/uv/)

Every Python environment in this project is described by a `pyproject.toml`, pinned by a
`uv.lock`, and installed with `uv`. There are two of them, because the two containers start from
different base images:

| Project | Container | Installed |
| --- | --- | --- |
| `pyproject.toml` (repository root) | Airflow components | At image build time, by the Dockerfile |
| `include/streamlit/pyproject.toml` | `streamlit` | At container start, by `uv sync` |

There is no `requirements.txt` to keep in sync: `uv` reads `pyproject.toml` directly in both
places. You do not need `uv` installed to follow the module - `astro dev start` builds and runs
everything on its own. You only need it to change a dependency, or to create the optional local
environment described further down.

### Changing the Airflow dependencies

Edit the root `pyproject.toml`, then run `uv lock` and `astro dev restart`. Which section to put
a package in:

- `[project] dependencies` for packages the Astro Runtime image does **not** ship. The Dockerfile
  installs these into the image.
- `[dependency-groups] runtime` for packages the image already ships (Airflow itself, pandas).
  These are pinned to the image's versions so the local environment matches the containers, and
  are deliberately not installed into the image.
- `[dependency-groups] dev` for tooling you only want locally, such as `pytest`.

> [!NOTE]
> The Dockerfile installs with `-c /etc/pip-constraints.txt`, the same constraint file Astro
> Runtime uses for its own dependencies, and installs only your direct dependencies so that `uv`
> resolves the rest against the hundreds of packages already in the image. Both details keep the
> install from moving versions that Airflow depends on.

> [!NOTE]
> The empty `requirements.txt` at the root has to stay. The Astro Runtime base image copies it in
> an `ONBUILD` trigger that runs before any instruction in our Dockerfile, and the build fails if
> the file is missing. Leaving it empty turns the image's own install step into a no-op.

### Changing the Streamlit dependencies

The frontend is a separate `uv` project, because it runs on a plain Python image where nothing is
pre-installed and its versions should not be constrained by Airflow's.

```bash
cd include/streamlit
uv add <package>     # or edit pyproject.toml and run 'uv lock'
```

Then `astro dev restart`. The container runs `uv sync --frozen` at start-up, installing exactly
what `include/streamlit/uv.lock` pins, and keeps its download cache in the `uv-cache` Docker
volume, so restarts after the first take a few seconds instead of a few minutes.

### Optional: a local environment for your editor

Everything runs inside Docker, so you never have to install Airflow on your machine. If you would
like editor autocompletion, type checking or the ability to run `pytest` outside the containers:

```bash
uv sync
```

This creates a `.venv` folder with Airflow 3.3.1, the Weaviate provider, LangChain and pandas at
the same versions the containers use. Point your editor at `.venv/bin/python`, and prefix commands
with `uv run` (for example `uv run pytest`) to use it from the terminal. Run `uv sync` inside
`include/streamlit/` as well if you want the same for the frontend code.

Commit both `uv.lock` files so that everyone resolves the same versions.

## Resources

### Airflow and AI/ML resources
- Academy: 
  - [Taskflow API Module](https://academy.astronomer.io/astro-runtime-taskflow)
  - [Airflow: Branching Module](https://academy.astronomer.io/astro-runtime-branching)
  - [Airflow: Dynamic Task Mapping Module](https://academy.astronomer.io/astro-runtime-dynamic-task-mapping)
- Learn Guides & Tutorials:
  - [Best practices for orchestrating MLOps pipelines with Airflow](https://www.astronomer.io/docs/learn/airflow-mlops)
  - [DAG-level parameters](https://www.astronomer.io/docs/learn/airflow-dag-parameters)
  - [Introduction to the TaskFlow API and Airflow decorators](https://www.astronomer.io/docs/learn/airflow-decorators)
  - [Branching in Airflow](https://www.astronomer.io/docs/learn/airflow-branch-operator)
  - [Create dynamic Airflow tasks](https://www.astronomer.io/docs/learn/dynamic-tasks)
  - [Orchestrate Weaviate operations with Apache Airflow](https://www.astronomer.io/docs/learn/airflow-weaviate)
- Other:
  - [Gen AI Cookbook](https://www.astronomer.io/ebooks/gen-ai-airflow-cookbook/) (Reference Architecture Diagrams)
  - [Ask Astro](https://github.com/astronomer/ask-astro)
  - [Advanced Content Generation Pipeline GitHub Repository](https://github.com/astronomer/gen-ai-fine-tune-rag-use-case)

### Documentation for the tools used:

- [Astronomer Airflow Learn Docs](https://www.astronomer.io/docs/learn)
- [Apache Airflow Official Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview)
- [Weaviate](https://weaviate.io/developers/weaviate)
- [Airflow Weaviate Provider](https://airflow.apache.org/docs/apache-airflow-providers-weaviate/stable/index.html)
- [Streamlit](https://docs.streamlit.io/)
- [LangChain](https://python.langchain.com/docs/introduction/)
- [pandas](https://pandas.pydata.org/docs/)
- [OpenAI](https://platform.openai.com/docs/introduction)
