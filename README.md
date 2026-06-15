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
```

**Important:** `BLA_FILEPATH` must be the path as it appears *inside the Docker container*, not on your local machine. The project directory is mounted into the container at `/opt/dagster/app`, so all data files under `Datasets/` are accessible at `/opt/dagster/app/Datasets/`.

### 9) Update `dagster.yaml` with your project path
The run launcher in `dagster.yaml` needs to know the absolute path to the project on your local machine so it can mount the code into pipeline run containers. Find this line near the bottom of the `container_kwargs.volumes` section:

```yaml
- /home/eloisewm/Projects/TEDISC-Dagster:/opt/dagster/app
```

Replace `/home/eloisewm/Projects/TEDISC-Dagster` with the absolute path to the project on your machine. You can find this by running:

```bash
pwd
```

from inside the project directory.

### 10) Build and run the containers
```bash
docker compose up --build
```

The `--build` flag rebuilds the Docker images from the Dockerfiles. It is only needed the first time, or after changes to a Dockerfile or `dagster.yaml`. For subsequent starts:
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
The following values differ per machine and must be set manually. They are not committed to the repo.

| Value | Where to set it | Notes |
| --- | --- | --- |
| `BLA_FILEPATH` | `.env` | Must use the container-internal path: `/opt/dagster/app/Datasets/...` |
| `DB_USER`, `DB_PASSWORD` | `.env` | Your personal database credentials |
| Project bind mount path | `dagster.yaml` under `container_kwargs.volumes` | Absolute path to the project root on your local machine |
| Docker socket path | `dagster.yaml` and `docker-compose.yaml` | See Server Deployment below |


## Server Deployment
The server (`dagster-dev.its.utas.edu.au`) uses rootless Docker, which means the Docker socket is at a different path than on a standard Linux machine. This affects the volume mounts in `dagster.yaml` and `docker-compose.yaml`.

On the server, the Docker socket path is `/run/user/<uid>/docker.sock` rather than `/var/run/docker.sock`. Search for all occurrences of `/var/run/docker.sock` in both files and replace with the correct path. Contact admin for the correct UID and any credential setup required.

The project bind mount path in `dagster.yaml` will also need to be updated to reflect the server's directory structure.


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
├── dagster.yaml
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
- `dagster.yaml` - Tells Dagster how to store run metadata and how to launch pipeline runs (via Docker). Committed to the repo. Contains two values that must be updated per machine: the Docker socket path and the project bind mount path (see Machine-specific values above).
- `workspace.yaml` - Tells the Dagster webserver where to find the pipeline code (the user code container on port 4004).
- `Dockerfile_dagster` - Docker image for the Dagster webserver and daemon.
- `Dockerfile_user_code` - Docker image for the pipeline code. Installs all project dependencies including the Microsoft SQL Server ODBC driver.
- `docker-compose.yaml` - Defines all four containers (PostgreSQL, webserver, daemon, user code) and how they connect to each other.
- `pyproject.toml` - Minimal project metadata file.