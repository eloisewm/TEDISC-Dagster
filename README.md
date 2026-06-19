## Project Summary
The ORE-TEDISC (Offshore Renewable Energy Trusted Environmental Data and Information Supply Chain) project aims to automate the processes currently being carried out manually under the NESP 4.7 project.

In brief, NESP 4.7 is looking to estimate the impact of newly proposed offshore windfarms in the Gippsland area on a set of priority species. This process involves a substantial effort in discovering and collating data, getting it model-ready, and feeding it through a system of interconnected models.

Through TEDISC, we hope to build a reproducible, auditable data supply chain; reducing manual effort, improving traceability, and making the workflow repeatable as new data becomes available or new wind farm proposals emerge.

Below is a high level overview of the NESP 4.7 workflow, and where the TEDISC project is targeting for automation (Products 1, 2 and 3):

![4.7 Workflow](docs/TEDISC%20Workflow.png)

The current focus is automating the retrieval and QAQC of species occurrence data for input into the species distribution models. The plan for the approach is documented here:

![Model Ready Data Workflow](docs/Model%20Ready%20Data%20Workflow.png)

TEDISC is built on Dagster, an open-source data orchestration platform. It manages the scheduling, execution, and monitoring of the data pipeline. The project runs entirely in Docker containers, both locally and on the server, to ensure a consistent and reproducible environment.


## First Time Setup

### 1) Install WSL
It is recommended to run the Dagster instance on WSL (Windows Subsystem for Linux) so that the local environment matches the server's Linux environment.

If you don't have it installed, open a Windows PowerShell terminal and run:
```bash
wsl --install -d Ubuntu
```
This installs WSL with Ubuntu as the distribution. Once it finishes, follow any prompts (a restart may be required).

After restarting, open a terminal and enter `wsl` to get onto the subsystem. The first time it opens it will ask you to create a username and password - this is your Linux account and does not need to match your Windows credentials.

This is now a Linux terminal - all subsequent steps are run from here.

### 2) Install VS Code and the WSL extension
If you don't have VS Code installed, download it from https://code.visualstudio.com/ and install on Windows as normal.

Once installed, go to the Extensions panel on the left (the four squares icon), search for "WSL", and install the extension.

The extension allows VS Code to run from inside the WSL.

### 3) Set up Git and GitHub
It is assumed that you have a GitHub account. If you do not have one, go to https://github.com/signup

Check if git is installed:
```bash
git --version
```

If not installed (i.e. you get `Command 'git' not found`), install with:
```bash
sudo apt install git
```

For a fresh install, tell git who you are. Run these two commands with the name you want to appear on commits and the email address associated with your GitHub account:
```bash
git config --global user.name "Your Name"
git config --global user.email "your@email.com"
```

### 4) Set up SSH keys for GitHub
SSH keys allow your machine to authenticate with GitHub without entering a password each time you push changes.

Check if you already have one:
```bash
ls ~/.ssh
```

If nothing with the format `id_ed25519` shows up, create one:
```bash
ssh-keygen -t ed25519 -C "your@email.com"
```

When prompted for a file location, hit Enter to accept the default. When prompted for a passphrase, hit Enter twice for no passphrase.

Get the contents of your public key:
```bash
cat ~/.ssh/id_ed25519.pub
```

Copy the entire output, then go to GitHub -> Settings -> SSH and GPG keys -> New SSH Key. Give it a title (e.g. "WSL"), paste the key, and click Add SSH Key.

Test that it worked:
```bash
ssh -T git@github.com
```

If it returns `Hi <username>! You've successfully authenticated`, you're good to go.

### 5) Clone the repo
Navigate to where you want to store the project:
```bash
cd /home/<your-username>/Projects/
```

Clone the repo:
```bash
git clone git@github.com:eloisewm/TEDISC-Dagster.git
cd TEDISC-Dagster
```

### 6) Install Docker and Docker Compose
```bash
sudo apt update
sudo apt install docker.io -y
sudo apt install docker-compose -y
```

Add your user to the Docker group so you don't need `sudo` every time:
```bash
sudo apt install util-linux-extra -y
sudo usermod -aG docker $USER
newgrp docker
```

### 7) Add input data files
The pipeline reads input data files from the `Datasets/` directory in the project root. This directory is not committed to the repo and must be created manually.

Create the directory and add the required files:
```bash
mkdir -p Datasets/BirdLifeAustralia
```

Then copy the BirdLife Australia CSV into that folder. The expected filename is `bla_output_csiro_290525.csv`.

### 8) Create your `.env` file
The project uses a `.env` file to store machine-specific configuration (credentials etc.) that differ between environments. This file is not committed to the repo and must be created manually.

In the project root (i.e. in `TEDISC-Dagster/`), create a file called `.env` and add the following:

```
BLA_FILEPATH=/opt/dagster/app/Datasets/BirdLifeAustralia/bla_output_csiro_290525.csv

DB_HOST=testimasag3.its.utas.edu.au
DB_PORT=1433
DB_NAME=IMASOREMonitoring
DB_USER=yourusername
DB_PASSWORD=yourpassword
DB_DRIVER=ODBC Driver 18 for SQL Server

# Only needed if your Docker socket is not at /var/run/docker.sock
# (e.g. rootless Docker / Podman). Otherwise leave this out.
# DOCKER_SOCK=/run/user/1000/docker.sock
```

**Important:** `BLA_FILEPATH` must be the path as it appears *inside the Docker container*, not on your local machine. The project directory is mounted into the container at `/opt/dagster/app`, so all data files under `Datasets/` are accessible at `/opt/dagster/app/Datasets/`.

### 9) (No manual `dagster.yaml` edit needed)
`dagster.yaml` is generated automatically at container startup from `dagster.yaml.tmpl` (see [How the instance config is templated](#how-the-instance-config-is-templated)). The two per-machine values — the Docker socket path and the project path — are supplied by the environment:

- **Project path:** defaults to the directory you run `docker compose` from (`$PWD`). To override, set `WORK_DIR=/absolute/path/to/TEDISC-Dagster` in `.env`.
- **Docker socket:** defaults to `/var/run/docker.sock`. To override, set `DOCKER_SOCK` in `.env` (see step 8).

### 10) Build and run the containers
```bash
docker compose up --build
```

The `--build` flag rebuilds the Docker images from the Dockerfiles. It is only needed the first time, or after changes to a Dockerfile or `dagster.yaml.tmpl`. For subsequent starts:
```bash
docker compose up
```

Open http://localhost:3004 in a browser to access the Dagster UI.

To stop the containers:
```bash
docker compose down
```

### 11) Open the project in VS Code
From the project directory:
```bash
code .
```

Do not open the project through File -> Open in Windows - it will cause path and environment issues.


## Starting a new session
```bash
sudo service docker start
cd ~/Projects/TEDISC-Dagster
docker compose up
```

Open http://localhost:3004 in a browser to access the Dagster UI. Open VS Code with `code .`

Note: Docker does not start automatically in WSL - you need to run `sudo service docker start` each session.


## Machine-specific values
The following values differ per machine. All are set via `.env` (not committed to the repo); there is no longer any need to hand-edit `dagster.yaml`.

| Value | Where to set it | Notes |
| --- | --- | --- |
| `BLA_FILEPATH` | `.env` | Must use the container-internal path: `/opt/dagster/app/Datasets/...` |
| `DB_USER`, `DB_PASSWORD` | `.env` | Your personal database credentials |
| Project bind mount path | `.env` → `WORK_DIR` (optional) | Defaults to `$PWD`; only set if you run compose from elsewhere |
| Docker socket path | `.env` → `DOCKER_SOCK` (optional) | Defaults to `/var/run/docker.sock`; see Server Deployment below |


## How the instance config is templated
Dagster's `dagster.yaml` does **not** support shell-style `${VAR}` substitution — only the `env:` key form (used for the Postgres credentials). To keep the per-machine values (Docker socket path, project path) out of the committed file, `dagster.yaml` is generated at container startup:

- `dagster.yaml.tmpl` is the committed template, with `${DOCKER_SOCK}` and `${WORK_DIR}` placeholders.
- The `Dockerfile_dagster` image runs `entrypoint.sh`, which uses `envsubst` to render `dagster.yaml.tmpl` → `dagster.yaml` (substituting only those two variables) before launching the webserver/daemon.
- The compose project name is pinned to `tedisc-dagster` (top-level `name:`), so the named volumes are always `tedisc-dagster_*` and the template can reference them by their full name.

To change the launcher config, edit `dagster.yaml.tmpl` and rebuild (`docker compose up --build`).


## Server Deployment
The server (`dagster-dev.its.utas.edu.au`) uses rootless Docker, which means the Docker socket is at a different path than on a standard Linux machine.

On the server, the Docker socket path is `/run/user/<uid>/docker.sock` rather than `/var/run/docker.sock`. Set `DOCKER_SOCK=/run/user/<uid>/docker.sock` in `.env` — this flows into both `docker-compose.yaml` and the templated `dagster.yaml`. Contact admin for the correct UID and any credential setup required.

The project path is taken from `$PWD` (or `WORK_DIR` in `.env`), so no manual path edits are needed on the server.


## Project Structure
```
TEDISC-Dagster/
├── tedisc_dagster/
│   ├── __init__.py
│   ├── definitions.py
│   └── defs/
│       ├── assets/
│       │   ├── __init__.py
│       │   ├── BirdLife.py
│       │   └── iNaturalist.py
│       ├── constants.py
│       ├── jobs.py
│       ├── resources.py
│       ├── schedules.py
│       ├── sensors.py
│       └── utils.py
├── Datasets/               # gitignored - create manually and add input data files
├── .env                    # gitignored - create from the template in step 8
├── docker-compose.yaml
├── Dockerfile_dagster
├── Dockerfile_user_code
├── dagster.yaml.tmpl       # template; rendered to dagster.yaml at container startup
├── entrypoint.sh           # renders dagster.yaml.tmpl via envsubst, then starts Dagster
├── workspace.yaml
├── pyproject.toml
└── README.md
```

- `tedisc_dagster/` - Python package containing all Dagster logic for the project.
- `__init__.py` - Marks the directory as a Python package. Required by Python but otherwise empty.
- `definitions.py` - Entry point for Dagster. Registers all assets, jobs, schedules, sensors, and resources into a single Definitions object that Dagster reads on startup.
- `assets/` - Contains asset definitions grouped by data source. In Dagster, an asset represents a persistent piece of data (a file, a database table, etc.) and the code that produces it.
- `BirdLife.py` - Assets for ingesting and processing species occurrence data from BirdLife Australia.
- `iNaturalist.py` - Assets for ingesting and processing species occurrence data from iNaturalist.
- `constants.py` - Stores project-wide constants such as file paths, species lists, and column names.
- `resources.py` - Configures shared resources such as database connections and API clients.
- `schedules.py` - Defines schedules that trigger jobs automatically at set times or intervals.
- `sensors.py` - Defines sensors that trigger jobs in response to external events.
- `utils.py` - Shared helper functions used across the project.
- `Datasets/` - Input data files. Not committed to the repo.
- `.env` - Stores machine-specific configuration. Not committed to the repo.
- `dagster.yaml.tmpl` - Template for Dagster's instance config (how to store run metadata and launch pipeline runs via Docker). Committed to the repo. Rendered to `dagster.yaml` inside the container at startup by `entrypoint.sh`, substituting the Docker socket path and project path from the environment (see [How the instance config is templated](#how-the-instance-config-is-templated)).
- `entrypoint.sh` - Container entrypoint for the webserver/daemon image; runs `envsubst` on `dagster.yaml.tmpl` then execs the Dagster process.
- `workspace.yaml` - Tells the Dagster webserver where to find the pipeline code (the user code container on port 4004).
- `Dockerfile_dagster` - Docker image for the Dagster webserver and daemon.
- `Dockerfile_user_code` - Docker image for the pipeline code. Installs all project dependencies including the Microsoft SQL Server ODBC driver.
- `docker-compose.yaml` - Defines all four containers (PostgreSQL, webserver, daemon, user code) and how they connect to each other.
- `pyproject.toml` - Minimal project metadata file.