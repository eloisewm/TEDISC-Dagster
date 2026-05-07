# TEDISC-Dagster

## First Time Setup
### 1) Install WSL
It is reccomended to install on the WSL (Windows Subsystem for Linux) so that our environment mimics the server's Linux environment. 

Open a Windows PowerShell terminal as an admin and run 
```bash
wsl --install
```

This will install WSL with Ubuntu (a version of Linux) by default. Once it finishes, follow any prompts (possible restart required).

After restarting, open Ubuntu from the start menu (or open powershell and type `wsl`). The first time it opens it will ask you to create a username and password. This is your Linux account - it doesnt need to match your Windows credentials. 

This is now a Linux terminal - all subsequent steps are run from here. 

### 2) Install VS Code and the WSL extension 
If you don't have VS Code installed, download from https://code.visualstudio.com/ and install on Windows as normal. 

Once installed, go to the Extensions panel on the left (the four squares icon), search for "WSL", and install the extension. 

The extension allows for VSCode to run from inside the WSL.

### 3) Set up Git and Github
It is assumed that you have set up a Github account. If you do not have one, go to https://github.com/signup

We will need to set up git to be able to clone the Github Repository to your pc
(Git is the thing running behind the scenes that actually tracks your changes - GitHub is just the website that lets you see and share what git is doing). 
In your terminal, check if git is intalled with 
```bash
git --version
```


If not installed (i.e. you get `Command 'git' not found`), install with
```bash 
sudo apt install git
```

For a fresh install, you will need to tell git who you are. Run these two commands with your name and the email address associated with your Github account:
```bash 
git config --global user.name "Your Name"
git config --global user.email "your@email.com"
```

### 4) Set Up SSH Keys for Github
SSH keys are how your machine authenticates with Github without needing to enter a password every time you want to push changes to your GitHub repo. You need to create a key on your machine and then give Github the public half of it.

Check if you already have one with:
```bash 
ls ~/.ssh
```

If nothing with the format id_ed25519 shows up (likely to be the case), create one with:
```bash 
ssh-keygen -t ed25519 -C "your@email.com"
```

When prompted with `Enter file in which to save the key`, just hit Enter to accept the default location.

When prompted for a passphrase, hit Enter twice for no passphrase.

Now get the contents of your public key:
```bash 
cat ~/.ssh/id_ed25519.pub # (cat opens the file in your terminal)
```

It should look something like this:
```
ssh-ed25519 AAAAC3NzaC1lZDI1NT*****/U eloisewilsonmayne@gmail.com
```

Copy the entire output and go to Github:
- Go to Settings
- Select SSH and GPG keys
- New SSH Key (give this a title like "WSL")
- Paste under Key
- Click Add SSH Key

Back in your teminal, test that this worked with 
```bash
ssh -T git@github.com
```

An expected output is:

```
The authenticity of host 'github.com (4.237.22.38)' can't be established.
ED25519 key fingerprint is SHA256:+DiY3wv****/zLD*****.
This key is not known by any other names.
Are you sure you want to continue connecting (yes/no/[fingerprint])?
```
type "yes"

If it returns:
```
Warning: Permanently added 'github.com' (ED25519) to the list of known hosts.
Hi eloisewm! You've successfully authenticated, but GitHub does not provide shell access.
```
then you're good to go.

### 5) Clone the Repo
In the terminal, navigate to where you want to store the project (you can use File Explorer to go into the WSL directories and create new folders). For example:
```bash
cd /home/eloisewm/Github/
```
Clone the repo: 
```bash
git clone https://github.com/eloisewm/TEDISC-Dagster
```

Now that the repo is synced, cd into it with
``` bash
cd TEDISC-Dagster
```

### 6) Install uv
uv is the pacakge manager used to manage Python dependencies for this project (i.e. instructs python to use specific version of specific packages that our project relies on)

Check if it is already installed with 
```bash 
uv --version
```

If not installed, install with:
```bash 
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Once installed, add it to your PATH so that your termianl can find it (PATH is a list of folders your terminal searches through when you type a command. It will allow us to type "uv" in the command line and instruct the terminal to use the functionalities of uv).
```bash 
source $HOME/.local/bin/env
```

### 7) Set Up the Python Environment
From inside the TEDISC-Dagster directory, run:
```bash
uv venv
source .venv/bin/activate
uv sync
```
- uv venv creates a virtual environment (an isolated Python installation just for this project)
- source .venv/bin/activate activates it - you will see (tedisc-dagster) appear to the left of your terminal prompt confirming it is active
- uv sync reads pyproject.toml and installs all the required packages

### 8) Run Dagster
```bash 
dagster dev
```
This starts a local Dagster instance. Open http://localhost:3000 in a browser
and you should be see the Dagster UI. 

Note that your terminal is now occcupied while Dagster is running. If you need to use a terminal, you will need to open a new one. You can close the dagster instance with Ctrl+C. 

### 9) Open the project in VS Code
From the project directory, run:
```bash 
code .
```

This opens VS Code from inside the WSL. Do not open the project through File -> Open in Windows as it will cause path and environemnent issues. 

## Starting a New Session
Each time you come back to work on the project, open an Ubuntu terminal:
```bash
cd ~Github/TEDISC-Dagster # (or wherever your project is)
source .venv/bin/activate
dasgster dev
```
Then open  http://localhost:3000 in your browser.

Open VS Code with:
```bash
code .
```

## Project Structure:
```
TEDISC-Dagster/
├── tedisc_dagster/         - Python package containing all Dagster logic
│   ├── __init__.py
│   ├── definitions.py
│   └── defs/
│       ├── assets.py
│       ├── constants.py   
│       ├── jobs.py
│       ├── resources.py
│       ├── schedules.py
│       ├── sensors.py
│       └── utils.py
├── pyproject.toml          - Project dependencies and configuration
├── uv.lock                 - Locked dependency versions (do not edit manually)
└── README.md
```




